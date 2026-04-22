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
  { url: "https://economymiddleeast.com/feed",                      source: "Economy Middle East" },
  { url: "https://www.arabianbusiness.com/feed",                    source: "Arabian Business" },
  { url: "https://www.emirates247.com/rss/mobile/v2/business.rss",  source: "Emirates247" },
];

const GNEWS_FEEDS = [
  { q: "zawya real estate UAE property",            source: "Zawya" },
  { q: "reuters UAE real estate property market",   source: "Reuters" },
  { q: "gulfbusiness.com UAE real estate",          source: "Gulf Business" },
  { q: "tradearabia UAE property real estate",      source: "Trade Arabia" },
  { q: "constructionweekonline UAE real estate",    source: "Construction Week" },
  { q: "khaleejtimes UAE property real estate",     source: "Khaleej Times" },
];

function gnewsUrl(q) {
  return `https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=en&gl=AE&ceid=AE:en`;
}

// ── HTML scrapers ─────────────────────────────────────────────────────────────

async function scrapeGulfNews() {
  const res = await fetch("https://gulfnews.com/business/property", {
    headers: { "User-Agent": UA, Accept: "text/html" },
    signal: AbortSignal.timeout(15_000),
  });
  if (!res.ok) throw new Error(`Gulf News HTTP ${res.status}`);
  const html = await res.text();
  const decode = (s) => s.replace(/&amp;/g, "&").replace(/&#x27;/g, "'").replace(/&quot;/g, '"').replace(/&lt;/g, "<").replace(/&gt;/g, ">");
  const articles = [];
  const seen = new Set();
  const hrefRe = /<a\s+href="(\/business\/property\/[^"]+)"/g;
  let m;
  while ((m = hrefRe.exec(html)) !== null) {
    const url = "https://gulfnews.com" + m[1];
    if (seen.has(url)) continue;
    const win = html.slice(m.index, m.index + 1400);
    const text = (raw) => decode(raw.replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim());
    const h1 = win.match(/<h1[^>]*>([\s\S]*?)<\/h1>/);
    const h2 = win.match(/<h2[^>]*>([\s\S]*?)<\/h2>/);
    const h3 = win.match(/<h3[^>]*>([\s\S]*?)<\/h3>/);
    const img = win.match(/<img\s+src="(\/\/[^"]+)"/);
    let title, summary;
    if (h1)      { title = text(h1[1]); summary = h2 ? text(h2[1]) : ""; }
    else if (h2) { title = text(h2[1]); summary = h3 ? text(h3[1]) : ""; }
    else continue;
    if (title.length < 20) continue;
    seen.add(url);
    articles.push({ source: "Gulf News", title, url, summary, imageUrl: img ? "https:" + img[1].split("?")[0] : "", publishedAt: new Date().toISOString() });
  }
  return articles;
}

async function scrapeKhaleejTimes() {
  const res = await fetch("https://www.khaleejtimes.com/business/property", {
    headers: { "User-Agent": UA, Accept: "text/html" },
    signal: AbortSignal.timeout(15_000),
  });
  if (!res.ok) throw new Error(`Khaleej Times HTTP ${res.status}`);
  const html = (await res.text()).replace(/&quot;/g, '"').replace(/&amp;/g, "&").replace(/&#x27;/g, "'").replace(/&lt;/g, "<").replace(/&gt;/g, ">");
  const headlineRe = /"headline":"([^"]{20,})"/g;
  const urlRe = /"url":"(https:\/\/www\.khaleejtimes\.com\/business\/property\/[^"]+)"/g;
  const imgRe = /"thumb_image":"([^"]+)"/g;
  const tsRe  = /"first_published_at":(\d{13})/g;
  const headlines = [], urls = [], images = [], timestamps = [];
  let mm;
  while ((mm = headlineRe.exec(html)) !== null) headlines.push({ pos: mm.index, val: mm[1] });
  while ((mm = urlRe.exec(html)) !== null)      urls.push({ pos: mm.index, val: mm[1] });
  while ((mm = imgRe.exec(html)) !== null)      images.push({ pos: mm.index, val: mm[1] });
  while ((mm = tsRe.exec(html)) !== null)       timestamps.push({ pos: mm.index, val: Number(mm[1]) });
  const nearest = (arr, pos) => arr.filter((x) => x.pos > pos).sort((a, b) => a.pos - b.pos)[0]?.val;
  const articles = [];
  const seen = new Set();
  for (const h of headlines) {
    const url = nearest(urls, h.pos);
    if (!url || seen.has(url)) continue;
    seen.add(url);
    const ts = nearest(timestamps, h.pos);
    articles.push({ source: "Khaleej Times", title: h.val, url, summary: "", imageUrl: (nearest(images, h.pos) ?? "").split("?")[0], publishedAt: ts ? new Date(ts).toISOString() : new Date().toISOString() });
  }
  return articles;
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

  for (const [fn, label] of [[scrapeGulfNews, "Gulf News"], [scrapeKhaleejTimes, "Khaleej Times"]]) {
    try {
      const articles = await fn();
      results.push(...articles);
      console.log(`[scrape] ${label}: ${articles.length} articles`);
    } catch (e) { console.warn(`[scrape] ${label} failed:`, e.message); }
  }

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

async function enrichWithImages(articles) {
  let dirty = false;

  const sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;

  // Step 1: Resolve unresolved GNews URLs (headless, 10 per cycle, recent only)
  const unresolvedGNews = articles
    .filter((a) => a.gnewsUrl && a.url.includes("news.google.com") && new Date(a.publishedAt).getTime() > sevenDaysAgo)
    .slice(0, 10);
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

  // Step 2: Fast HTTP og:image for non-blocked, non-GNews articles
  const fast = articles.filter((a) => !a.imageUrl && !a.url.includes("news.google.com") && !HEADLESS_DOMAINS.some((d) => a.url.includes(d))).slice(0, 200);
  if (fast.length) {
    console.log(`[images] fast: ${fast.length} articles...`);
    let enriched = 0;
    for (let i = 0; i < fast.length; i += 6) {
      await Promise.allSettled(fast.slice(i, i + 6).map(async (a) => {
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

  // Step 3: Headless og:image for Cloudflare-protected domains (AB, Reuters etc.), recent only
  const slow = articles
    .filter((a) => !a.imageUrl && HEADLESS_DOMAINS.some((d) => a.url.includes(d)) && new Date(a.publishedAt).getTime() > sevenDaysAgo)
    .slice(0, 15);
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
