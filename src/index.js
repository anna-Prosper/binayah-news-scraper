import http from "http";
import crypto from "crypto";
import RssParser from "rss-parser";
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import chromium from "@sparticuz/chromium";
import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";

puppeteer.use(StealthPlugin());
chromium.setHeadlessMode = true;
chromium.setGraphicsMode = false;

const PORT = process.env.PORT || 3001;
const CACHE_TTL_MS = 30 * 60 * 1000;
const UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
const parser = new RssParser({ timeout: 12_000, headers: { "User-Agent": UA } });

// ── S3 ────────────────────────────────────────────────────────────────────────

const s3 = new S3Client({
  region: process.env.AWS_REGION || "ap-south-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});
const BUCKET = process.env.AWS_S3_BUCKET || "binayah-media";

async function s3Get(key) {
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
    return JSON.parse(await res.Body.transformToString());
  } catch { return null; }
}

async function s3Put(key, data) {
  try {
    await s3.send(new PutObjectCommand({
      Bucket: BUCKET, Key: key,
      Body: JSON.stringify(data),
      ContentType: "application/json",
    }));
  } catch (e) { console.warn("[s3] put failed:", e.message); }
}

// gnewsUrl → real article URL (persists so we never resolve the same GNews URL twice)
let urlCache = {};
// real article URL → S3 image URL (persists so we never re-upload the same image)
let imageCache = {};

async function loadCaches() {
  const [u, i] = await Promise.all([s3Get("news/url-cache.json"), s3Get("news/image-cache.json")]);
  urlCache   = u || {};
  imageCache = i || {};
  console.log(`[s3] url-cache: ${Object.keys(urlCache).length}, image-cache: ${Object.keys(imageCache).length}`);
}

async function saveCaches() {
  await Promise.all([s3Put("news/url-cache.json", urlCache), s3Put("news/image-cache.json", imageCache)]);
}

async function uploadImage(ogUrl, articleUrl) {
  try {
    const res = await fetch(ogUrl, {
      headers: { "User-Agent": UA },
      signal: AbortSignal.timeout(10_000),
      redirect: "follow",
    });
    if (!res.ok) return "";
    const ct = res.headers.get("content-type") || "image/jpeg";
    if (!ct.startsWith("image/")) return "";
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length < 2000) return ""; // skip tiny placeholders / 1px trackers
    const ext = ct.includes("png") ? "png" : ct.includes("webp") ? "webp" : "jpg";
    const hash = crypto.createHash("md5").update(articleUrl).digest("hex");
    const key = `news/images/${hash}.${ext}`;
    await s3.send(new PutObjectCommand({
      Bucket: BUCKET, Key: key, Body: buf,
      ContentType: ct, CacheControl: "public, max-age=31536000",
    }));
    return `https://${BUCKET}.s3.${process.env.AWS_REGION || "ap-south-1"}.amazonaws.com/${key}`;
  } catch { return ""; }
}

// ── RSS sources ───────────────────────────────────────────────────────────────

const RSS_FEEDS = [
  { url: "https://www.arabianbusiness.com/feed", source: "Arabian Business" },
];

const GNEWS_FEEDS = [
  { q: "gulfnews.com property real estate UAE",              source: "Gulf News" },
  { q: "zawya real estate UAE property",                     source: "Zawya" },
  { q: "reuters UAE real estate property market",            source: "Reuters" },
  { q: "gulfbusiness.com UAE real estate",                   source: "Gulf Business" },
  { q: "tradearabia.com UAE property real estate",           source: "Trade Arabia" },
  { q: "constructionweekonline UAE real estate",             source: "Construction Week" },
  { q: "khaleejtimes UAE property real estate",              source: "Khaleej Times" },
  { q: "economymiddleeast.com property real estate UAE",     source: "Economy Middle East" },
  { q: "emirates247.com property real estate UAE",           source: "Emirates247" },
];

function gnewsUrl(q) {
  return `https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=en&gl=AE&ceid=AE:en`;
}

// ── Fetch all sources ─────────────────────────────────────────────────────────

async function fetchAll() {
  const results = [];

  await Promise.allSettled(RSS_FEEDS.map(async ({ url, source }) => {
    try {
      const feed = await parser.parseURL(url);
      for (const item of feed.items ?? []) {
        if (!item.title || !item.link) continue;
        results.push({ source, title: item.title.trim(), url: item.link, summary: (item.contentSnippet || item.summary || "").slice(0, 300), imageUrl: item.enclosure?.url || item["media:content"]?.["$"]?.url || "", publishedAt: item.pubDate ? new Date(item.pubDate).toISOString() : new Date().toISOString() });
      }
      console.log(`[rss] ${source}: ${feed.items?.length ?? 0} items`);
    } catch (e) { console.warn(`[rss] ${source} failed:`, e.message); }
  }));

  await Promise.allSettled(GNEWS_FEEDS.map(async ({ q, source }) => {
    try {
      const feed = await parser.parseURL(gnewsUrl(q));
      for (const item of feed.items ?? []) {
        if (!item.title || !item.link) continue;
        // Use cached real URL if we've resolved this GNews link before
        const realUrl = urlCache[item.link] || item.link;
        results.push({ source, title: item.title.trim(), url: realUrl, gnewsUrl: item.link, summary: "", imageUrl: imageCache[realUrl] || "", publishedAt: item.pubDate ? new Date(item.pubDate).toISOString() : new Date().toISOString() });
      }
      console.log(`[gnews] ${source}: ${feed.items?.length ?? 0} items`);
    } catch (e) { console.warn(`[gnews] ${source} failed:`, e.message); }
  }));

  const RE_KEYWORDS = /real.?estate|property|properties|mortgage|rent|landlord|tenant|apartment|villa|residential|commercial|off.?plan|handover|developer|realty|housing|sqft|sq\.ft|dubai land|DLD|RERA|leasehold|freehold/i;
  const seen = new Set();
  return results
    .filter((a) => {
      const key = a.gnewsUrl || a.url;
      if (seen.has(key)) return false;
      seen.add(key);
      return RE_KEYWORDS.test(a.title) || RE_KEYWORDS.test(a.summary || "");
    })
    .sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
}

// ── Headless browser ──────────────────────────────────────────────────────────

let _browser = null;
async function getBrowser() {
  if (_browser?.connected) return _browser;
  _browser = await puppeteer.launch({
    args: chromium.args,
    defaultViewport: { width: 1280, height: 720 },
    executablePath: await chromium.executablePath(),
    headless: true,
  });
  _browser.on("disconnected", () => { _browser = null; });
  return _browser;
}

async function withPage(fn) {
  let page;
  try {
    const b = await getBrowser();
    page = await b.newPage();
    return await fn(page);
  } catch (e) {
    console.warn("[headless] error:", e.message?.slice(0, 100));
    return null;
  } finally {
    await page?.close().catch(() => {});
  }
}

// Resolve a GNews redirect URL to the real article URL using headless
async function resolveGNewsUrl(gnewsArticleUrl) {
  return withPage(async (page) => {
    await page.setDefaultNavigationTimeout(15_000);
    await page.goto(gnewsArticleUrl, { waitUntil: "domcontentloaded" });
    const finalUrl = page.url();
    return !finalUrl.includes("news.google.com") ? finalUrl : null;
  });
}

// Get og:image from a URL using headless (for Cloudflare-protected domains)
async function ogImageHeadless(url) {
  return withPage(async (page) => {
    await page.setDefaultNavigationTimeout(20_000);
    await page.goto(url, { waitUntil: "domcontentloaded" });
    const img = await page.evaluate(() => document.querySelector('meta[property="og:image"]')?.getAttribute("content") || "");
    return img.startsWith("http") ? img : null;
  });
}

// ── og:image via plain HTTP ───────────────────────────────────────────────────

const BAD_IMAGE_HOSTS = ["lh3.googleusercontent.com", "news.google.com"];

async function ogImageFetch(url) {
  try {
    const res = await fetch(url, { headers: { "User-Agent": UA, Accept: "text/html" }, signal: AbortSignal.timeout(8_000), redirect: "follow" });
    const html = await res.text();
    const m = html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i) ||
              html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']/i);
    const img = m?.[1] || "";
    if (!img.startsWith("http")) return "";
    if (BAD_IMAGE_HOSTS.some((h) => img.includes(h))) return "";
    return img;
  } catch { return ""; }
}

// ── Enrichment pipeline ───────────────────────────────────────────────────────

const HEADLESS_DOMAINS = ["arabianbusiness.com", "reuters.com", "constructionweekonline.com"];
let enrichmentRunning = false;

async function enrichWithImages(articles) {
  if (enrichmentRunning) return;
  enrichmentRunning = true;
  try {
    let dirty = false;
    const sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;

    // Step 1: Resolve unresolved GNews URLs (headless, 50 per cycle, recent only)
    const unresolvedGNews = articles
      .filter((a) => a.gnewsUrl && a.url.includes("news.google.com") && new Date(a.publishedAt).getTime() > sevenDaysAgo)
      .slice(0, 50);
    if (unresolvedGNews.length) {
      console.log(`[urls] resolving ${unresolvedGNews.length} GNews URLs...`);
      let resolved = 0;
      for (const a of unresolvedGNews) {
        const realUrl = await resolveGNewsUrl(a.gnewsUrl);
        if (realUrl) {
          urlCache[a.gnewsUrl] = realUrl;
          a.url = realUrl;
          if (imageCache[realUrl]) a.imageUrl = imageCache[realUrl];
          dirty = true;
          resolved++;
        }
      }
      console.log(`[urls] resolved ${resolved}/${unresolvedGNews.length}`);
      await _browser?.close().catch(() => {}); _browser = null;
    }

    // Step 2: Fast HTTP og:image for all non-blocked, non-GNews articles (no cap)
    const fast = articles.filter((a) => !a.imageUrl && !a.url.includes("news.google.com") && !HEADLESS_DOMAINS.some((d) => a.url.includes(d)));
    if (fast.length) {
      console.log(`[images] fast: ${fast.length} articles...`);
      let enriched = 0;
      for (let i = 0; i < fast.length; i += 8) {
        await Promise.allSettled(fast.slice(i, i + 8).map(async (a) => {
          const ogUrl = await ogImageFetch(a.url);
          if (!ogUrl) return;
          const s3Url = await uploadImage(ogUrl, a.url);
          a.imageUrl = s3Url || ogUrl;
          imageCache[a.url] = a.imageUrl;
          dirty = true;
          enriched++;
        }));
      }
      console.log(`[images] fast enriched ${enriched}/${fast.length}`);
    }

    // Step 3: Headless og:image for Cloudflare-protected domains, recent only
    const slow = articles
      .filter((a) => !a.imageUrl && HEADLESS_DOMAINS.some((d) => a.url.includes(d)) && new Date(a.publishedAt).getTime() > sevenDaysAgo)
      .slice(0, 20);
    if (slow.length) {
      console.log(`[images] headless: ${slow.length} articles...`);
      let enriched = 0;
      for (const a of slow) {
        const ogUrl = await ogImageHeadless(a.url);
        if (!ogUrl) continue;
        const s3Url = await uploadImage(ogUrl, a.url);
        a.imageUrl = s3Url || ogUrl;
        imageCache[a.url] = a.imageUrl;
        dirty = true;
        enriched++;
      }
      console.log(`[images] headless enriched ${enriched}/${slow.length}`);
      await _browser?.close().catch(() => {}); _browser = null;
    }

    if (dirty) await saveCaches();
  } finally {
    enrichmentRunning = false;
  }
}

// ── Cache ─────────────────────────────────────────────────────────────────────

let cache = { data: null, fetchedAt: 0 };

async function getNews(limit = 50) {
  if (!cache.data || Date.now() - cache.fetchedAt > CACHE_TTL_MS) {
    console.log("[cache] refreshing...");
    cache.data = await fetchAll();
    cache.fetchedAt = Date.now();
    console.log(`[cache] ${cache.data.length} articles cached`);
    enrichWithImages(cache.data).catch((e) => console.warn("[enrich] failed:", e.message));
  }
  return cache.data.slice(0, limit);
}

// Run enrichment every 5 min so images fill in continuously between cache refreshes
setInterval(() => {
  if (cache.data?.length) {
    enrichWithImages(cache.data).catch((e) => console.warn("[enrich] interval failed:", e.message));
  }
}, 5 * 60 * 1000);

loadCaches()
  .then(() => getNews())
  .catch((e) => console.warn("Startup failed:", e.message));

// ── HTTP server ───────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Content-Type", "application/json");

  if (req.method === "GET" && url.pathname === "/health") {
    const bySource = {};
    for (const a of cache.data ?? []) bySource[a.source] = (bySource[a.source] ?? 0) + 1;
    res.writeHead(200);
    return res.end(JSON.stringify({ ok: true, cached: !!cache.data, articles: cache.data?.length ?? 0, urlCacheSize: Object.keys(urlCache).length, imageCacheSize: Object.keys(imageCache).length, fetchedAt: cache.fetchedAt ? new Date(cache.fetchedAt).toISOString() : null, bySource }));
  }

  if (req.method === "GET" && url.pathname === "/news") {
    try {
      const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "50"), 1000);
      const source = url.searchParams.get("source");
      let articles = await getNews(1000);
      if (source) articles = articles.filter((a) => a.source.toLowerCase().includes(source.toLowerCase()));
      res.writeHead(200);
      return res.end(JSON.stringify({ ok: true, count: articles.slice(0, limit).length, fetchedAt: new Date(cache.fetchedAt).toISOString(), articles: articles.slice(0, limit) }));
    } catch (e) {
      res.writeHead(500);
      return res.end(JSON.stringify({ ok: false, error: e.message }));
    }
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: "Not found. Use GET /news or GET /health" }));
});

server.listen(PORT, () => console.log(`binayah-news-scraper listening on port ${PORT}`));
