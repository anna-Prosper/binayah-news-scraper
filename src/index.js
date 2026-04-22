import http from "http";
import crypto from "crypto";
import RssParser from "rss-parser";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { MongoClient } from "mongodb";
import chromium from "@sparticuz/chromium";
import puppeteerCore from "puppeteer-core";
import { addExtra } from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";

// Wire puppeteer-extra to puppeteer-core explicitly (no vanilla puppeteer installed)
const puppeteer = addExtra(puppeteerCore);
puppeteer.use(StealthPlugin());
chromium.setHeadlessMode = true;
chromium.setGraphicsMode = false;

const PORT = process.env.PORT || 3001;
const CACHE_TTL_MS = 30 * 60 * 1000;
const UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
const parser = new RssParser({ timeout: 12_000, headers: { "User-Agent": UA } });

// ── S3 (images only) ─────────────────────────────────────────────────────────

const s3 = new S3Client({
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});
const BUCKET = process.env.AWS_S3_BUCKET || "binayah-media-456051253184-us-east-1-an";

// ── MongoDB caches ───────────────────────────────────────────────────────────
// Keyed lookups persist across restarts in `news_scraper` DB (shared cluster).
// Collections:  url_cache { _id: gnewsUrl, url }
//              image_cache { _id: articleUrl, imageUrl }
//              body_cache  { _id: articleUrl, body }

const MONGODB_URI = process.env.MONGODB_URI;
let mongoClient = null;
let urlCol, imageCol, bodyCol;

let urlCache   = {}; // gnewsUrl → real article URL
let imageCache = {}; // real article URL → image URL (S3 or original)
let bodyCache  = {}; // real article URL → article body text (≤1500 chars)

// Writes queued between flushes to avoid hammering Mongo per enrichment step.
const pendingWrites = { url: new Map(), image: new Map(), body: new Map() };

async function loadCaches() {
  if (!MONGODB_URI) {
    console.warn("[mongo] MONGODB_URI not set — caches will not persist");
    return;
  }
  mongoClient = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 8000 });
  await mongoClient.connect();
  // Use the default DB the user has permissions for (binayah_web_new_dev).
  // Collection names prefixed to namespace scraper caches.
  const db = mongoClient.db();
  urlCol   = db.collection("newsScraper_urlCache");
  imageCol = db.collection("newsScraper_imageCache");
  bodyCol  = db.collection("newsScraper_bodyCache");

  const [urls, images, bodies] = await Promise.all([
    urlCol.find({}, { projection: { _id: 1, url: 1 } }).toArray(),
    imageCol.find({}, { projection: { _id: 1, imageUrl: 1 } }).toArray(),
    bodyCol.find({}, { projection: { _id: 1, body: 1 } }).toArray(),
  ]);
  for (const r of urls)   urlCache[r._id]   = r.url;
  for (const r of images) imageCache[r._id] = r.imageUrl;
  for (const r of bodies) bodyCache[r._id]  = r.body;
  console.log(`[mongo] url-cache: ${urls.length}, image-cache: ${images.length}, body-cache: ${bodies.length}`);
}

function queueUrl(k, v)   { urlCache[k]   = v; pendingWrites.url.set(k, v); }
function queueImage(k, v) { imageCache[k] = v; pendingWrites.image.set(k, v); }
function queueBody(k, v)  { bodyCache[k]  = v; pendingWrites.body.set(k, v); }

async function flushCaches() {
  if (!mongoClient) return;
  const tasks = [];
  if (pendingWrites.url.size) {
    const ops = [...pendingWrites.url].map(([k, v]) => ({ updateOne: { filter: { _id: k }, update: { $set: { url: v } }, upsert: true } }));
    pendingWrites.url.clear();
    tasks.push(urlCol.bulkWrite(ops, { ordered: false }).catch((e) => console.warn("[mongo] url flush:", e.message)));
  }
  if (pendingWrites.image.size) {
    const ops = [...pendingWrites.image].map(([k, v]) => ({ updateOne: { filter: { _id: k }, update: { $set: { imageUrl: v } }, upsert: true } }));
    pendingWrites.image.clear();
    tasks.push(imageCol.bulkWrite(ops, { ordered: false }).catch((e) => console.warn("[mongo] image flush:", e.message)));
  }
  if (pendingWrites.body.size) {
    const ops = [...pendingWrites.body].map(([k, v]) => ({ updateOne: { filter: { _id: k }, update: { $set: { body: v } }, upsert: true } }));
    pendingWrites.body.clear();
    tasks.push(bodyCol.bulkWrite(ops, { ordered: false }).catch((e) => console.warn("[mongo] body flush:", e.message)));
  }
  await Promise.all(tasks);
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
    return `https://${BUCKET}.s3.${process.env.AWS_REGION || "us-east-1"}.amazonaws.com/${key}`;
  } catch { return ""; }
}

// ── RSS sources ───────────────────────────────────────────────────────────────

const RSS_FEEDS = [
  { url: "https://www.arabianbusiness.com/feed",                          source: "Arabian Business" },
  { url: "https://economymiddleeast.com/feed",                            source: "Economy Middle East" },
  { url: "https://www.emirates247.com/rss/mobile/v2/business.rss",       source: "Emirates247" },
  { url: "https://www.propertyfinder.ae/blog/feed/",                      source: "Property Finder" },
];

const GNEWS_FEEDS = [
  { q: "gulfnews.com property real estate UAE",              source: "Gulf News" },
  { q: "zawya real estate UAE property",                     source: "Zawya" },
  { q: "reuters UAE real estate property market",            source: "Reuters" },
  { q: "gulfbusiness.com UAE real estate",                   source: "Gulf Business" },
  { q: "tradearabia.com UAE property real estate",           source: "Trade Arabia" },
  { q: "constructionweekonline UAE real estate",             source: "Construction Week" },
  { q: "khaleejtimes UAE property real estate",              source: "Khaleej Times" },
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
        results.push({ source, title: item.title.trim(), url: item.link, summary: (item.contentSnippet || item.summary || "").slice(0, 300), imageUrl: item.enclosure?.url || item["media:content"]?.["$"]?.url || "", body: bodyCache[item.link] || "", publishedAt: item.pubDate ? new Date(item.pubDate).toISOString() : new Date().toISOString() });
      }
      console.log(`[rss] ${source}: ${feed.items?.length ?? 0} items`);
    } catch (e) { console.warn(`[rss] ${source} failed:`, e.message); }
  }));

  await Promise.allSettled(GNEWS_FEEDS.map(async ({ q, source }) => {
    try {
      const feed = await parser.parseURL(gnewsUrl(q));
      for (const item of feed.items ?? []) {
        if (!item.title || !item.link) continue;
        // Decode GNews URL at ingest — cache hit first, then base64 decode, else keep redirect URL
        const realUrl = urlCache[item.link] || resolveGNewsUrl(item.link) || item.link;
        results.push({ source, title: item.title.trim(), url: realUrl, gnewsUrl: item.link, summary: "", imageUrl: imageCache[realUrl] || "", body: bodyCache[realUrl] || "", publishedAt: item.pubDate ? new Date(item.pubDate).toISOString() : new Date().toISOString() });
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

// Resolve a GNews redirect URL to the real article URL.
// GNews embeds the target URL in the base64-encoded article path (protobuf field).
// We decode it directly — no HTTP request, no headless browser.
function resolveGNewsUrl(gnewsArticleUrl) {
  try {
    const match = gnewsArticleUrl.match(/articles\/([A-Za-z0-9_-]+)/);
    if (!match) return null;
    const buf = Buffer.from(match[1].replace(/-/g, "+").replace(/_/g, "/"), "base64");
    const str = buf.toString("binary");
    const idx = str.indexOf("https://");
    if (idx === -1) return null;
    let end = str.indexOf("\x00", idx);
    if (end === -1) end = str.length;
    const url = str.slice(idx, end).replace(/[\x00-\x1f]/g, "");
    return url.startsWith("http") ? url : null;
  } catch { return null; }
}

// Resolve a GNews URL to the real article URL using headless
async function resolveGNewsHeadless(gnewsUrl) {
  return withPage(async (page) => {
    await page.setDefaultNavigationTimeout(15_000);
    await page.goto(gnewsUrl, { waitUntil: "domcontentloaded" });
    let finalUrl = page.url();
    if (finalUrl.includes("news.google.com")) {
      await page.waitForNavigation({ timeout: 5000 }).catch(() => {});
      finalUrl = page.url();
    }
    return finalUrl.includes("news.google.com") ? null : finalUrl;
  });
}

// Get og:image from a URL using headless (for Cloudflare-protected domains)
async function ogImageHeadless(url) {
  return withPage(async (page) => {
    await page.setDefaultNavigationTimeout(20_000);
    await page.goto(url, { waitUntil: "domcontentloaded" });
    const img = await page.evaluate(() =>
      document.querySelector('meta[property="og:image"]')?.getAttribute("content") ||
      document.querySelector('meta[name="twitter:image"]')?.getAttribute("content") || ""
    );
    return img.startsWith("http") ? img : null;
  });
}

// ── Page data (og:image + article body) via plain HTTP ───────────────────────

const BAD_IMAGE_HOSTS = ["lh3.googleusercontent.com", "news.google.com"];
const SKIP_BODY_DOMAINS = ["arabianbusiness.com", "reuters.com"];

function cleanBodyText(text) {
  return text
    .replace(/<[^>]+>/g, " ")
    .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&#x27;/g, "'").replace(/&nbsp;/g, " ")
    .replace(/\s+/g, " ").trim()
    .slice(0, 1500);
}

function extractBody(html) {
  // 1. JSON-LD articleBody (most reliable — structured data)
  for (const m of html.matchAll(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)) {
    try {
      const d = JSON.parse(m[1]);
      const items = Array.isArray(d) ? d : [d, ...(d["@graph"] || [])];
      for (const item of items) {
        if (item?.articleBody) return cleanBodyText(item.articleBody);
      }
    } catch {}
  }
  // 2. <article> tag paragraphs
  const artHtml = html.match(/<article[^>]*>([\s\S]*?)<\/article>/i)?.[1] || "";
  if (artHtml) {
    const paras = [...artHtml.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/gi)]
      .map((m) => m[1].replace(/<[^>]+>/g, "").trim())
      .filter((p) => p.length > 40);
    if (paras.length >= 2) return cleanBodyText(paras.join(" "));
  }
  // 3. Largest paragraph cluster in full page
  const paras = [...html.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/gi)]
    .map((m) => m[1].replace(/<[^>]+>/g, "").trim())
    .filter((p) => p.length > 60);
  if (paras.length >= 2) return cleanBodyText(paras.slice(0, 7).join(" "));
  return "";
}

async function fetchPageData(url) {
  try {
    const res = await fetch(url, { headers: { "User-Agent": UA, Accept: "text/html" }, signal: AbortSignal.timeout(10_000), redirect: "follow" });
    if (!res.ok) return { imageUrl: "", body: "" };
    const html = await res.text();
    const m = html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i) ||
              html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']/i) ||
              html.match(/<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i) ||
              html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+name=["']twitter:image["']/i);
    const img = m?.[1] || "";
    const imageUrl = img.startsWith("http") && !BAD_IMAGE_HOSTS.some((h) => img.includes(h)) ? img : "";
    const body = SKIP_BODY_DOMAINS.some((d) => url.includes(d)) ? "" : extractBody(html);
    return { imageUrl, body };
  } catch { return { imageUrl: "", body: "" }; }
}

// ── Enrichment pipeline ───────────────────────────────────────────────────────

const HEADLESS_DOMAINS = ["arabianbusiness.com", "reuters.com"];
let enrichmentRunning = false;

async function enrichWithImages(articles) {
  if (enrichmentRunning) return;
  enrichmentRunning = true;
  try {
    let dirty = false;
    const sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;

    // Step 1: Resolve unresolved GNews URLs — base64 decode first, headless fallback
    const unresolvedGNews = articles
      .filter((a) => a.gnewsUrl && a.url.includes("news.google.com") && new Date(a.publishedAt).getTime() > sevenDaysAgo)
      .slice(0, 30);
    if (unresolvedGNews.length) {
      console.log(`[urls] resolving ${unresolvedGNews.length} GNews URLs...`);
      let resolved = 0;
      for (const a of unresolvedGNews) {
        let realUrl = resolveGNewsUrl(a.gnewsUrl);
        if (!realUrl) realUrl = await resolveGNewsHeadless(a.gnewsUrl);
        if (realUrl) {
          queueUrl(a.gnewsUrl, realUrl);
          a.url = realUrl;
          if (imageCache[realUrl]) a.imageUrl = imageCache[realUrl];
          resolved++;
        }
      }
      console.log(`[urls] resolved ${resolved}/${unresolvedGNews.length}`);
      await _browser?.close().catch(() => {}); _browser = null;
      await flushCaches();
    }

    // Step 2: Fetch page data (image + body) for all open-access articles in one request
    const fast = articles.filter((a) =>
      (!a.imageUrl || !a.body) &&
      !a.url.includes("news.google.com") &&
      !HEADLESS_DOMAINS.some((d) => a.url.includes(d))
    );
    if (fast.length) {
      console.log(`[enrich] fast: ${fast.length} articles...`);
      let imgCount = 0, bodyCount = 0;
      for (let i = 0; i < fast.length; i += 16) {
        await Promise.allSettled(fast.slice(i, i + 16).map(async (a) => {
          const { imageUrl, body } = await fetchPageData(a.url);
          if (imageUrl && !a.imageUrl) {
            const s3Url = await uploadImage(imageUrl, a.url);
            a.imageUrl = s3Url || imageUrl;
            queueImage(a.url, a.imageUrl);
            imgCount++;
          }
          if (body && !a.body) {
            a.body = body;
            queueBody(a.url, body);
            bodyCount++;
          }
        }));
        // Flush every batch so progress is durable even if the process dies mid-run.
        await flushCaches();
      }
      console.log(`[enrich] fast done — images: ${imgCount}, bodies: ${bodyCount}/${fast.length}`);
    }

    // Step 3: Headless og:image for Cloudflare-protected domains, recent only
    const slow = articles
      .filter((a) => !a.imageUrl && HEADLESS_DOMAINS.some((d) => a.url.includes(d)) && new Date(a.publishedAt).getTime() > sevenDaysAgo)
      .slice(0, 30);
    if (slow.length) {
      console.log(`[images] headless: ${slow.length} articles...`);
      let enriched = 0;
      for (const a of slow) {
        const ogUrl = await ogImageHeadless(a.url);
        if (!ogUrl) continue;
        const s3Url = await uploadImage(ogUrl, a.url);
        a.imageUrl = s3Url || ogUrl;
        queueImage(a.url, a.imageUrl);
        enriched++;
      }
      console.log(`[images] headless enriched ${enriched}/${slow.length}`);
      await _browser?.close().catch(() => {}); _browser = null;
      await flushCaches();
    }
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

// Run enrichment every 2 min so images fill in continuously between cache refreshes
setInterval(() => {
  if (cache.data?.length) {
    enrichWithImages(cache.data).catch((e) => console.warn("[enrich] interval failed:", e.message));
  }
}, 2 * 60 * 1000);

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
    return res.end(JSON.stringify({ ok: true, cached: !!cache.data, articles: cache.data?.length ?? 0, urlCacheSize: Object.keys(urlCache).length, imageCacheSize: Object.keys(imageCache).length, bodyCacheSize: Object.keys(bodyCache).length, fetchedAt: cache.fetchedAt ? new Date(cache.fetchedAt).toISOString() : null, bySource }));
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
