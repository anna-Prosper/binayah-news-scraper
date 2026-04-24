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
  try {
    mongoClient = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 8000 });
    await mongoClient.connect();
    // Dedicated DB for scraper state — separate from app data.
    const db = mongoClient.db(process.env.MONGODB_DB || "db_migration");
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
  } catch (e) {
    console.warn("[mongo] load failed, continuing without persistence:", e.message);
    mongoClient = null;  // flushCaches() will no-op
    urlCol = imageCol = bodyCol = null;
  }
}

function queueUrl(k, v)   { urlCache[k]   = v; pendingWrites.url.set(k, v); }
function queueImage(k, v) { imageCache[k] = v; pendingWrites.image.set(k, v); }
function queueBody(k, v)  { bodyCache[k]  = v; pendingWrites.body.set(k, v); }

async function flushCaches() {
  if (!mongoClient || !urlCol) return;
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
    const s3Url = `https://${BUCKET}.s3.${process.env.AWS_REGION || "us-east-1"}.amazonaws.com/${key}`;
    console.log(`[s3] uploaded ${key} (${buf.length}b)`);
    return s3Url;
  } catch (e) {
    console.warn(`[s3] uploadImage failed for ${articleUrl.slice(0, 60)}: ${e.message}`);
    return "";
  }
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
        // Prefer cached (S3/enriched) image over RSS enclosure — the enclosure
        // field is sometimes empty or churns as the feed rotates, but imageCache
        // persists in Mongo across refreshes.
        const rssImage = item.enclosure?.url || item["media:content"]?.["$"]?.url || "";
        const cachedImage = imageCache[item.link];
        const imageUrl = cachedImage || rssImage;
        // Persist the RSS enclosure URL so it survives feed rotation — but only
        // if we don't already have a better (enriched) one cached.
        if (!cachedImage && rssImage) queueImage(item.link, rssImage);
        results.push({ source, title: item.title.trim(), url: item.link, summary: (item.contentSnippet || item.summary || "").slice(0, 300), imageUrl, body: bodyCache[item.link] || "", publishedAt: item.pubDate ? new Date(item.pubDate).toISOString() : new Date().toISOString() });
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
  const twoWeeksAgo = Date.now() - 14 * 24 * 60 * 60 * 1000;
  const MIN_ARTICLES = 30;

  // Deduplicate and keyword-filter first
  const seen = new Set();
  const relevant = results.filter((a) => {
    const key = a.gnewsUrl || a.url;
    if (seen.has(key)) return false;
    seen.add(key);
    return RE_KEYWORDS.test(a.title) || RE_KEYWORDS.test(a.summary || "");
  }).sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));

  // Apply 2-week cutoff; fall back to most recent MIN_ARTICLES if that leaves too few
  const recent = relevant.filter((a) => new Date(a.publishedAt).getTime() >= twoWeeksAgo);
  return recent.length >= MIN_ARTICLES ? recent : relevant.slice(0, MIN_ARTICLES);
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

// Domains whose og:image must be fetched via Vercel proxy (Render IPs blocked by Cloudflare)
const VERCEL_OG_DOMAINS = ["khaleejtimes.com"];
const VERCEL_OG_PROXY = "https://binayah-news-dashboard.vercel.app/api/fetch-og";

async function fetchOgViaVercel(url) {
  try {
    const r = await fetch(`${VERCEL_OG_PROXY}?url=${encodeURIComponent(url)}`, {
      signal: AbortSignal.timeout(15_000),
    });
    if (!r.ok) return { imageUrl: "", body: "" };
    const json = await r.json();
    return {
      imageUrl: typeof json.imageUrl === "string" && json.imageUrl.startsWith("http") ? json.imageUrl : "",
      body: typeof json.body === "string" ? json.body : "",
    };
  } catch { return { imageUrl: "", body: "" }; }
}

// Resolve a GNews URL via Google's batchexecute RPC — port of Python's
// googlenewsdecoder v1 (new_decoderv1). Single POST returns the real
// publisher URL. Uses Chrome UA (Googlebot gets blocked on this endpoint).
const CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36";

// Tier 0: Vercel proxy — delegates batchexecute to Vercel's IP pool (not blocked by Google unlike Render's IPs)
const VERCEL_PROXY = "https://binayah-news-dashboard.vercel.app/api/resolve-gnews";
async function resolveGNewsViaVercel(gnewsUrl) {
  try {
    const m = gnewsUrl.match(/articles\/([A-Za-z0-9_-]+)/);
    if (!m) return null;
    const r = await fetch(`${VERCEL_PROXY}?id=${encodeURIComponent(m[1])}`, {
      signal: AbortSignal.timeout(15_000),
    });
    if (!r.ok) {
      console.warn(`[vercel-proxy] HTTP ${r.status} for ${m[1]}`);
      return null;
    }
    const json = await r.json();
    return typeof json.url === "string" && json.url.startsWith("http") ? json.url : null;
  } catch (e) {
    console.warn(`[vercel-proxy] error: ${e.message}`);
    return null;
  }
}

let batchDebugLogged = false;
async function resolveGNewsBatch(gnewsUrl) {
  try {
    const m = gnewsUrl.match(/articles\/([A-Za-z0-9_-]+)/);
    if (!m) return null;
    const articleId = m[1];

    // 1. Fetch article page for signature + timestamp (try /articles/, fallback /rss/articles/)
    let html = null;
    let lastStatus = 0;
    for (const prefix of ["/articles/", "/rss/articles/"]) {
      try {
        const r = await fetch(`https://news.google.com${prefix}${articleId}`, {
          headers: { "User-Agent": CHROME_UA },
          signal: AbortSignal.timeout(10_000),
        });
        lastStatus = r.status;
        if (r.ok) { html = await r.text(); break; }
      } catch (e) { lastStatus = -1; }
    }
    if (!html) {
      if (!batchDebugLogged) { console.warn(`[batch] GET page failed, status=${lastStatus}`); batchDebugLogged = true; }
      return null;
    }
    // Python uses selectolax to find `c-wiz > div[jscontroller]` and read its
    // data-n-a-sg / data-n-a-ts. We match that div's attrs via regex.
    const divMatch = html.match(/<c-wiz[^>]*>\s*<div[^>]+jscontroller[^>]*>/);
    const region = divMatch ? html.slice(divMatch.index, divMatch.index + 2000) : html;
    const sig = region.match(/data-n-a-sg="([^"]+)"/)?.[1];
    const ts  = region.match(/data-n-a-ts="([^"]+)"/)?.[1];
    if (!sig || !ts) {
      if (!batchDebugLogged) {
        console.warn(`[batch] sig/ts missing — html len=${html.length}, divMatch=${!!divMatch}, htmlSample=${html.slice(0,200).replace(/\s+/g,' ')}`);
        batchDebugLogged = true;
      }
      return null;
    }

    // 2. POST batchexecute — payload order is [articleId, timestamp, signature]
    //    (Not sig/ts/articleId — that was my earlier bug)
    const innerPayload = [
      "Fbv4je",
      `["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,null,null,null,null,null,0,1],"X","X",1,[1,1,1],1,1,null,0,0,null,0],"${articleId}",${ts},"${sig}"]`,
    ];
    const body = "f.req=" + encodeURIComponent(JSON.stringify([[innerPayload]]));

    const res = await fetch("https://news.google.com/_/DotsSplashUi/data/batchexecute", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "User-Agent": CHROME_UA,
      },
      body,
      signal: AbortSignal.timeout(10_000),
    });
    if (!res.ok) {
      if (!batchDebugLogged) { console.warn(`[batch] POST failed status=${res.status}`); batchDebugLogged = true; }
      return null;
    }
    const text = await res.text();

    // Response format: `)]}'\n\n<len>\n[[...]]\n<len>\n...`.
    // Python does: parsed = json.loads(text.split("\n\n")[1])[:-2]
    //              decoded_url = json.loads(parsed[0][2])[1]
    const parts = text.split("\n\n");
    if (parts.length < 2) {
      if (!batchDebugLogged) { console.warn(`[batch] response parse: parts=${parts.length}, sample=${text.slice(0,200)}`); batchDebugLogged = true; }
      return null;
    }
    const outer = JSON.parse(parts[1]);
    // outer[0][2] is a JSON-encoded string; position 1 of that parsed array is the URL
    const innerStr = outer[0]?.[2];
    if (typeof innerStr !== "string") return null;
    const inner = JSON.parse(innerStr);
    const url = inner?.[1];
    return typeof url === "string" && url.startsWith("http") ? url : null;
  } catch { return null; }
}

// Tier 2: Headless click — render the GNews article page, find the publisher
// <a href> in the DOM, return it (or click + wait for navigation).
async function resolveGNewsClick(gnewsUrl) {
  return withPage(async (page) => {
    await page.setDefaultNavigationTimeout(20_000);
    try {
      await page.goto(gnewsUrl, { waitUntil: "networkidle2", timeout: 20_000 });
    } catch { /* partial load is fine if DOM has the link */ }

    // Maybe already redirected
    let u = page.url();
    if (!u.includes("news.google.com")) return u;

    // Grab any external publisher link from the DOM
    const href = await page.evaluate(() => {
      const skip = ["google.", "youtube.", "gstatic.", "googleapis."];
      for (const a of document.querySelectorAll('a[href^="http"]')) {
        const h = a.href;
        if (!skip.some((d) => h.includes(d))) return h;
      }
      return null;
    }).catch(() => null);
    if (href) return href;

    // Fallback: click a main-area link and wait for navigation
    try {
      await Promise.all([
        page.waitForNavigation({ timeout: 8_000 }).catch(() => {}),
        page.click('c-wiz a, article a, main a').catch(() => {}),
      ]);
      u = page.url();
      if (!u.includes("news.google.com")) return u;
    } catch {}
    return null;
  });
}

// Tier 3: Navigate + poll — last resort for when click tier also fails
async function resolveGNewsHeadless(gnewsUrl) {
  return withPage(async (page) => {
    await page.setDefaultNavigationTimeout(25_000);
    try {
      await page.goto(gnewsUrl, { waitUntil: "domcontentloaded" });
    } catch { /* still check current URL below */ }
    for (let i = 0; i < 16; i++) {
      const u = page.url();
      if (!u.includes("news.google.com")) return u;
      await new Promise((r) => setTimeout(r, 500));
    }
    return null;
  });
}

// Get og:image from a URL using headless (for Cloudflare-protected domains)
async function ogImageHeadless(url) {
  return withPage(async (page) => {
    await page.setDefaultNavigationTimeout(30_000);
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    } catch { /* still try to read meta if partial load */ }
    const img = await page.evaluate(() =>
      document.querySelector('meta[property="og:image"]')?.getAttribute("content") ||
      document.querySelector('meta[name="twitter:image"]')?.getAttribute("content") || ""
    ).catch(() => "");
    return img && img.startsWith("http") ? img : null;
  });
}

// ── Authenticated headless scraping (Arabian Business login) ──────────────────

const AB_EMAIL    = process.env.AB_EMAIL;
const AB_PASSWORD = process.env.AB_PASSWORD;

let _abCookies = null;
let _abLoginAt  = 0;
const AB_SESSION_TTL = 4 * 60 * 60 * 1000; // re-login every 4 h

async function waitForCFClear(page, timeoutMs = 12_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const title = await page.title().catch(() => "");
    if (!title.toLowerCase().includes("just a moment")) return true;
    await new Promise((r) => setTimeout(r, 1000));
  }
  return false;
}

async function ensureABLogin() {
  if (!AB_EMAIL || !AB_PASSWORD) return false;
  if (_abCookies && Date.now() - _abLoginAt < AB_SESSION_TTL) return true;

  console.log("[ab-login] logging in...");
  const cookies = await withPage(async (page) => {
    await page.setDefaultNavigationTimeout(35_000);

    // Step 1: Load homepage first — stealth resolves CF clearance cookie here
    try { await page.goto("https://www.arabianbusiness.com", { waitUntil: "domcontentloaded", timeout: 25_000 }); } catch {}
    await waitForCFClear(page);
    console.log(`[ab-login] homepage loaded, title="${(await page.title().catch(() => "?")).slice(0, 40)}"`);

    // Step 2: Navigate to login page (CF clearance cookie now set)
    try { await page.goto("https://www.arabianbusiness.com/login", { waitUntil: "domcontentloaded", timeout: 25_000 }); } catch {}
    const cfOk = await waitForCFClear(page);
    const loginTitle = await page.title().catch(() => "?");
    console.log(`[ab-login] login page: title="${loginTitle.slice(0, 40)}", cfOk=${cfOk}`);

    // Step 3: Fill in credentials
    // AB uses WordPress login: input#user_login (name="log") + input#user_pass (name="pwd")
    const emailSel = '#user_login, input[name="log"], input[type="email"], input[name="email"]';
    const passSel  = '#user_pass, input[name="pwd"], input[type="password"]';
    const submitSel = '#wp-submit, input[name="wp-submit"], button[type="submit"], input[type="submit"]';

    try { await page.waitForSelector(emailSel, { timeout: 10_000 }); } catch {
      console.warn("[ab-login] email field not found — page may be CF-blocked or login URL changed");
      // Log visible inputs for diagnosis
      const inputs = await page.evaluate(() =>
        [...document.querySelectorAll("input")].map((i) => `${i.type}|${i.name}|${i.id}`).join(", ")
      ).catch(() => "");
      console.warn("[ab-login] inputs on page:", inputs.slice(0, 200));
      return null;
    }

    await page.click(emailSel);
    await page.type(emailSel, AB_EMAIL, { delay: 60 });
    await page.click(passSel);
    await page.type(passSel, AB_PASSWORD, { delay: 60 });

    await Promise.all([
      page.waitForNavigation({ timeout: 15_000, waitUntil: "domcontentloaded" }).catch(() => {}),
      page.click(submitSel).catch(() => page.keyboard.press("Enter")),
    ]);

    const postUrl = page.url();
    const c = await page.cookies();
    console.log(`[ab-login] post-login url=${postUrl.slice(0, 70)}, cookies=${c.length}`);
    return c.length > 3 ? c : null;
  });

  if (cookies) {
    _abCookies = cookies;
    _abLoginAt  = Date.now();
    console.log("[ab-login] session established");
    return true;
  }
  console.warn("[ab-login] login failed — will retry next cycle");
  return false;
}

async function scrapeABBody(url) {
  if (!_abCookies) return "";
  return (await withPage(async (page) => {
    await page.setCookie(..._abCookies);
    await page.setDefaultNavigationTimeout(30_000);
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    } catch { /* partial load — still try to extract */ }

    if (page.url().includes("/login")) {
      _abCookies = null; // session expired — force re-login next cycle
      return "";
    }

    const body = await page.evaluate(() => {
      const SELECTORS = [
        ".article__body", ".article-body", ".article-content",
        '[class*="article-body"]', '[class*="articleBody"]', "article",
      ];
      for (const sel of SELECTORS) {
        const el = document.querySelector(sel);
        if (!el) continue;
        const paras = [...el.querySelectorAll("p")]
          .map((p) => p.textContent.trim())
          .filter((p) => p.length > 40);
        if (paras.length >= 2) return paras.slice(0, 8).join(" ");
      }
      const paras = [...document.querySelectorAll("p")]
        .map((p) => p.textContent.trim())
        .filter((p) => p.length > 60);
      return paras.slice(0, 7).join(" ");
    }).catch(() => "");

    return body || "";
  })) || "";
}

// ── Page data (og:image + article body) via plain HTTP ───────────────────────

const BAD_IMAGE_HOSTS = ["lh3.googleusercontent.com", "news.google.com", "KT.jpg"];
const SKIP_BODY_DOMAINS = ["reuters.com"];

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
    const res = await fetch(url, { headers: { "User-Agent": CHROME_UA, Accept: "text/html,application/xhtml+xml,*/*;q=0.9" }, signal: AbortSignal.timeout(10_000), redirect: "follow" });
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

    // Step 1: Resolve unresolved GNews URLs in three tiers —
    //   (1) batchexecute RPC (fast, no browser, 8-way parallel)
    //   (2) headless + find publisher link in DOM (click tier)
    //   (3) headless + poll page.url() for JS redirect (last resort)
    // Base64 decode is a free pre-check for older URL formats.
    const unresolvedGNews = articles
      .filter((a) => a.gnewsUrl && a.url.includes("news.google.com"))
      .slice(0, 100);
    if (unresolvedGNews.length) {
      console.log(`[urls] resolving ${unresolvedGNews.length} GNews URLs...`);
      let resolved = 0, viaVercel = 0, viaBatch = 0, viaB64 = 0, viaClick = 0, viaHeadless = 0;

      // Tier 0: Vercel proxy (parallel batches of 8) — avoids Render's blocked IPs
      for (let i = 0; i < unresolvedGNews.length; i += 8) {
        await Promise.allSettled(unresolvedGNews.slice(i, i + 8).map(async (a) => {
          const realUrl = await resolveGNewsViaVercel(a.gnewsUrl);
          if (!realUrl) return;
          viaVercel++;
          queueUrl(a.gnewsUrl, realUrl);
          a.url = realUrl;
          if (imageCache[realUrl]) a.imageUrl = imageCache[realUrl];
          resolved++;
        }));
        await flushCaches();
      }

      // Tier 1: direct batchexecute fallback (parallel batches of 8)
      const stillUnresolved1 = unresolvedGNews.filter((a) => a.url.includes("news.google.com"));
      for (let i = 0; i < stillUnresolved1.length; i += 8) {
        await Promise.allSettled(stillUnresolved1.slice(i, i + 8).map(async (a) => {
          const realUrl = (await resolveGNewsBatch(a.gnewsUrl)) || resolveGNewsUrl(a.gnewsUrl);
          if (!realUrl) return;
          if (realUrl !== resolveGNewsUrl(a.gnewsUrl)) viaBatch++; else viaB64++;
          queueUrl(a.gnewsUrl, realUrl);
          a.url = realUrl;
          if (imageCache[realUrl]) a.imageUrl = imageCache[realUrl];
          resolved++;
        }));
        await flushCaches();
      }

      // Tier 2: click — for any still unresolved recent articles (capped — each takes ~20s)
      const stillUnresolved2 = unresolvedGNews.filter((a) => a.url.includes("news.google.com")).slice(0, 15);
      for (const a of stillUnresolved2) {
        const realUrl = await resolveGNewsClick(a.gnewsUrl);
        if (realUrl) {
          queueUrl(a.gnewsUrl, realUrl);
          a.url = realUrl;
          if (imageCache[realUrl]) a.imageUrl = imageCache[realUrl];
          resolved++; viaClick++;
        }
      }

      // Tier 3: poll — absolute last resort
      const stillUnresolved3 = unresolvedGNews.filter((a) => a.url.includes("news.google.com")).slice(0, 5);
      for (const a of stillUnresolved3) {
        const realUrl = await resolveGNewsHeadless(a.gnewsUrl);
        if (realUrl) {
          queueUrl(a.gnewsUrl, realUrl);
          a.url = realUrl;
          if (imageCache[realUrl]) a.imageUrl = imageCache[realUrl];
          resolved++; viaHeadless++;
        }
      }

      console.log(`[urls] resolved ${resolved}/${unresolvedGNews.length} (vercel:${viaVercel} batch:${viaBatch} b64:${viaB64} click:${viaClick} headless:${viaHeadless})`);
      await _browser?.close().catch(() => {}); _browser = null;
      await flushCaches();
    }

    // Step 1.5: Vercel og:image proxy for domains blocked by Cloudflare on Render IPs
    const vercelOg = articles.filter((a) =>
      (!a.imageUrl || !a.body) && !a.url.includes("news.google.com") &&
      VERCEL_OG_DOMAINS.some((d) => a.url.includes(d))
    );
    if (vercelOg.length) {
      console.log(`[enrich] vercel-og: ${vercelOg.length} articles...`);
      let voImg = 0, voBody = 0;
      for (let i = 0; i < vercelOg.length; i += 8) {
        await Promise.allSettled(vercelOg.slice(i, i + 8).map(async (a) => {
          const { imageUrl, body } = await fetchOgViaVercel(a.url);
          if (imageUrl && !BAD_IMAGE_HOSTS.some((h) => imageUrl.includes(h)) && !a.imageUrl) {
            const s3Url = await uploadImage(imageUrl, a.url);
            a.imageUrl = s3Url || imageUrl;
            queueImage(a.url, a.imageUrl);
            voImg++;
          }
          if (body && !a.body) {
            a.body = body;
            queueBody(a.url, body);
            voBody++;
          }
        }));
        await flushCaches();
      }
      console.log(`[enrich] vercel-og done — images: ${voImg}/${vercelOg.length}, bodies: ${voBody}`);
    }

    // Step 2.5: Arabian Business — scrape body via logged-in headless session
    const abNeedBody = articles
      .filter((a) => !a.body && a.url.includes("arabianbusiness.com"))
      .slice(0, 15); // cap per cycle — each page takes ~5s
    if (abNeedBody.length) {
      console.log(`[ab-login] ${abNeedBody.length} articles need body...`);
      const loggedIn = await ensureABLogin();
      if (loggedIn) {
        let abBodyCount = 0;
        for (const a of abNeedBody) {
          const body = await scrapeABBody(a.url);
          if (body) {
            a.body = body.slice(0, 1500);
            queueBody(a.url, a.body);
            abBodyCount++;
          }
        }
        console.log(`[ab-login] scraped ${abBodyCount}/${abNeedBody.length}`);
        await flushCaches();
        await _browser?.close().catch(() => {}); _browser = null;
      }
    }

    // Step 2: Fetch page data (image + body) for all open-access articles in one request
    const fast = articles.filter((a) =>
      (!a.imageUrl || !a.body) &&
      !a.url.includes("news.google.com") &&
      !HEADLESS_DOMAINS.some((d) => a.url.includes(d)) &&
      !VERCEL_OG_DOMAINS.some((d) => a.url.includes(d))
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

    // Step 3: Headless og:image for Cloudflare-protected domains.
    // No date filter — AB's full backlog (~128 articles) deserves enrichment,
    // and imageCache persists so it's a one-time cost per article.
    const slow = articles
      .filter((a) => !a.imageUrl && HEADLESS_DOMAINS.some((d) => a.url.includes(d)))
      .slice(0, 40);
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

// ── Tanami floor-plan scraper ────────────────────────────────────────────────
// Scrapes /Projects/{slug}-FloorPlans pages from tanamiproperties.com, uploads
// images to S3, caches results in MongoDB. Tanami is CF-protected and each
// CF clearance is single-page, so we use a fresh incognito context per visit.

const TANAMI_SLUGS = [
  "Kanyon-by-Beyond",
  "Orise-by-Beyond",
  "Saria-by-Beyond",
  "Sensia-by-Beyond",
  "The-Mural-by-Beyond",
  "Talea-at-Dubai-Maritime-City",
  "Haven-by-Aldar",
  "Zuha-Island-Villas",
  "Santorini-at-Damac-Lagoons",
  "Golf-Point-at-Emaar-South-Dubai",
  "Beach-Mansion",
];

const TANAMI_BASE = "https://www.tanamiproperties.com";
let tanamiCache = { data: null, fetchedAt: 0 };
let tanamiCol = null;
let tanamiRefreshRunning = false;
const TANAMI_TTL_MS = 12 * 60 * 60 * 1000; // 12h

// Dedicated Tanami browser — isolated from the shared `_browser` used by news
// enrichment (which closes between runs and would kill Tanami mid-scrape).
let _tanamiBrowser = null;
async function getTanamiBrowser() {
  if (_tanamiBrowser?.connected) return _tanamiBrowser;
  _tanamiBrowser = await puppeteer.launch({
    args: chromium.args,
    defaultViewport: { width: 1366, height: 900 },
    executablePath: await chromium.executablePath(),
    headless: true,
  });
  _tanamiBrowser.on("disconnected", () => { _tanamiBrowser = null; });
  return _tanamiBrowser;
}

// @sparticuz/chromium runs --single-process, which doesn't support multiple
// BrowserContexts. Instead, we use the default context and clear cookies/cache
// before each navigation to simulate a fresh session (needed because Tanami's
// CF only clears one navigation per cookie state).
async function withFreshTanamiContext(fn) {
  const b = await getTanamiBrowser();
  let page;
  try {
    page = await b.newPage();
    const cdp = await page.target().createCDPSession();
    await cdp.send("Network.clearBrowserCookies").catch(() => {});
    await cdp.send("Network.clearBrowserCache").catch(() => {});
    await page.setUserAgent(CHROME_UA);
    await page.setExtraHTTPHeaders({
      "Accept-Language": "en-US,en;q=0.9",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    });
    return await fn(page);
  } finally {
    await page?.close().catch(() => {});
  }
}

async function tanamiGotoAndClearCF(page, url) {
  await page.setDefaultNavigationTimeout(40_000);
  try { await page.goto(url, { waitUntil: "domcontentloaded", timeout: 40_000 }); } catch {}
  for (let i = 0; i < 15; i++) {
    const t = await page.title().catch(() => "");
    if (!/just a moment|attention required/i.test(t)) break;
    await new Promise((r) => setTimeout(r, 1000));
  }
  await new Promise((r) => setTimeout(r, 1500));
  const title = await page.title().catch(() => "");
  return !/attention required|just a moment/i.test(title);
}

async function uploadTanamiImage(originalUrl) {
  try {
    const res = await fetch(originalUrl, {
      headers: { "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(15_000),
      redirect: "follow",
    });
    if (!res.ok) return "";
    const ct = res.headers.get("content-type") || "image/jpeg";
    if (!ct.startsWith("image/")) return "";
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length < 2000) return "";
    const ext = ct.includes("png") ? "png" : ct.includes("webp") ? "webp" : "jpg";
    const hash = crypto.createHash("md5").update(originalUrl).digest("hex");
    const key = `tanami/floor-plans/${hash}.${ext}`;
    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: buf,
        ContentType: ct,
        CacheControl: "public, max-age=31536000",
      })
    );
    return `https://${BUCKET}.s3.${process.env.AWS_REGION || "us-east-1"}.amazonaws.com/${key}`;
  } catch {
    return "";
  }
}

async function scrapeTanamiOverview(slug) {
  return withFreshTanamiContext(async (page) => {
    const ok = await tanamiGotoAndClearCF(page, `${TANAMI_BASE}/Projects/${slug}`);
    if (!ok) return null;
    return page.evaluate(() => {
      const summary = {};
      for (const tr of document.querySelectorAll("#SummarId table tr")) {
        const th = (tr.querySelector("th")?.textContent || "").replace(/:/g, "").trim().toLowerCase();
        const td = (tr.querySelector("td")?.textContent || "").trim();
        if (th && td) summary[th.replace(/\s+/g, "_")] = td;
      }
      return { pageTitle: document.title || "", summary };
    });
  });
}

async function scrapeTanamiFloorPlans(slug) {
  return withFreshTanamiContext(async (page) => {
    const ok = await tanamiGotoAndClearCF(page, `${TANAMI_BASE}/Projects/${slug}-FloorPlans`);
    if (!ok) return [];

    // scroll to trigger lazy image loads
    await page.evaluate(async () => {
      await new Promise((r) => {
        let y = 0;
        const step = () => {
          window.scrollTo(0, y);
          y += 500;
          if (y < document.body.scrollHeight) setTimeout(step, 60); else r();
        };
        step();
      });
    }).catch(() => {});
    await new Promise((r) => setTimeout(r, 1000));

    return page.evaluate(() => {
      const getHeaders = (tbl) => {
        const thCells = [...tbl.querySelectorAll("thead th")];
        if (thCells.length) return thCells.map((x) => (x.textContent || "").trim().toLowerCase());
        const firstRow = tbl.querySelector("tr");
        return firstRow ? [...firstRow.querySelectorAll("th, td")].map((x) => (x.textContent || "").trim().toLowerCase()) : [];
      };
      const target = [...document.querySelectorAll("table")].find((t) => {
        const h = getHeaders(t);
        return h.includes("floor plan") && h.includes("sizes");
      });
      if (!target) return [];
      const headers = getHeaders(target);
      const hasTheadTh = target.querySelectorAll("thead th").length > 0;
      const idx = {
        image: headers.indexOf("floor plan"),
        category: headers.indexOf("category"),
        unitType: headers.indexOf("unit type"),
        floorDetails: headers.indexOf("floor details"),
        sizes: headers.indexOf("sizes"),
        type: headers.indexOf("type"),
      };
      const dataRows = hasTheadTh ? [...target.querySelectorAll("tbody tr")] : [...target.querySelectorAll("tr")].slice(1);
      const get = (cells, i) => (i >= 0 && cells[i] ? (cells[i].textContent || "").trim() : "");
      const out = [];
      for (const tr of dataRows) {
        const cells = [...tr.querySelectorAll("td")];
        if (cells.length < Math.max(idx.image, idx.sizes) + 1) continue;
        const imgCell = cells[idx.image];
        const img = imgCell?.querySelector("img");
        const a = imgCell?.querySelector("a");
        if (!img && !a) continue;
        const originalImageUrl = a?.href || img?.getAttribute("data-echo") || img?.src || "";
        out.push({
          originalImageUrl,
          alt: img?.alt || "",
          category: get(cells, idx.category),
          unitType: get(cells, idx.unitType),
          floorDetails: get(cells, idx.floorDetails),
          sizes: get(cells, idx.sizes),
          type: get(cells, idx.type),
        });
      }
      return out;
    });
  });
}

async function refreshTanami() {
  if (tanamiRefreshRunning) return;
  tanamiRefreshRunning = true;
  console.log("[tanami] refresh starting...");
  try {
    const projects = [];
    for (const slug of TANAMI_SLUGS) {
      try {
        console.log(`[tanami] ${slug} — overview`);
        const overview = await scrapeTanamiOverview(slug);
        await new Promise((r) => setTimeout(r, 1500));
        console.log(`[tanami] ${slug} — floor plans`);
        const rawPlans = await scrapeTanamiFloorPlans(slug);

        // Upload each image to S3 (parallel batches of 4)
        const plans = [];
        for (let i = 0; i < rawPlans.length; i += 4) {
          const batch = rawPlans.slice(i, i + 4);
          const results = await Promise.all(
            batch.map(async (p) => {
              if (!p.originalImageUrl) return p;
              const s3Url = await uploadTanamiImage(p.originalImageUrl);
              return { ...p, imageUrl: s3Url || p.originalImageUrl };
            })
          );
          plans.push(...results);
        }

        projects.push({
          slug,
          url: `${TANAMI_BASE}/Projects/${slug}`,
          floorPlansUrl: `${TANAMI_BASE}/Projects/${slug}-FloorPlans`,
          title: overview?.pageTitle || slug.replace(/-/g, " "),
          summary: overview?.summary || {},
          floorPlans: plans,
        });
        console.log(`[tanami] ${slug} — ${plans.length} plans, ${plans.filter((p) => p.imageUrl && p.imageUrl.includes(BUCKET)).length} uploaded`);
        await new Promise((r) => setTimeout(r, 2000));
      } catch (e) {
        console.warn(`[tanami] ${slug} failed: ${e.message?.slice(0, 100)}`);
        projects.push({ slug, error: e.message?.slice(0, 120) });
      }
    }
    tanamiCache = { data: projects, fetchedAt: Date.now() };
    if (tanamiCol) {
      try {
        await tanamiCol.updateOne(
          { _id: "snapshot" },
          { $set: { projects, fetchedAt: new Date() } },
          { upsert: true }
        );
        console.log("[tanami] saved to mongo");
      } catch (e) {
        console.warn("[tanami] mongo save:", e.message);
      }
    }
    const totalPlans = projects.reduce((n, p) => n + (p.floorPlans?.length || 0), 0);
    console.log(`[tanami] refresh complete — ${projects.length} projects, ${totalPlans} floor plans`);
  } finally {
    tanamiRefreshRunning = false;
    // Close our dedicated browser — frees memory on Render's small instances
    await _tanamiBrowser?.close().catch(() => {}); _tanamiBrowser = null;
  }
}

async function loadTanamiCache() {
  if (!mongoClient) return;
  try {
    const db = mongoClient.db(process.env.MONGODB_DB || "db_migration");
    tanamiCol = db.collection("newsScraper_tanamiCache");
    const doc = await tanamiCol.findOne({ _id: "snapshot" });
    if (doc?.projects) {
      tanamiCache = { data: doc.projects, fetchedAt: doc.fetchedAt?.getTime?.() || Date.now() };
      console.log(`[tanami] loaded from mongo — ${doc.projects.length} projects`);
    }
  } catch (e) {
    console.warn("[tanami] mongo load:", e.message);
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
    // Flush RSS enclosure URLs captured during fetchAll so they're durable
    // before enrichment starts (enrichment also flushes, but this guarantees
    // feed rotation can't strip cached images between runs).
    flushCaches().catch((e) => console.warn("[mongo] initial flush:", e.message));
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
  .then(async () => {
    await loadTanamiCache();
    getNews().catch((e) => console.warn("News startup:", e.message));
    // Kick off tanami refresh if cache is empty or stale
    if (!tanamiCache.data || Date.now() - tanamiCache.fetchedAt > TANAMI_TTL_MS) {
      setTimeout(() => {
        refreshTanami().catch((e) => console.warn("[tanami] startup refresh:", e.message));
      }, 30_000); // wait 30s after startup so news enrichment gets priority
    }
  })
  .catch((e) => console.warn("Startup failed:", e.message));

// Refresh Tanami every 12h
setInterval(() => {
  refreshTanami().catch((e) => console.warn("[tanami] interval refresh:", e.message));
}, TANAMI_TTL_MS);

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
      // Patch missing bodies from cache (in-memory body may lag behind bodyCache)
      for (const a of articles) {
        if (!a.body && bodyCache[a.url]) a.body = bodyCache[a.url];
        if (!a.imageUrl && imageCache[a.url]) a.imageUrl = imageCache[a.url];
      }
      if (source) articles = articles.filter((a) => a.source.toLowerCase().includes(source.toLowerCase()));
      res.writeHead(200);
      return res.end(JSON.stringify({ ok: true, count: articles.slice(0, limit).length, fetchedAt: new Date(cache.fetchedAt).toISOString(), articles: articles.slice(0, limit) }));
    } catch (e) {
      res.writeHead(500);
      return res.end(JSON.stringify({ ok: false, error: e.message }));
    }
  }

  if (req.method === "GET" && url.pathname === "/tanami") {
    // Trigger refresh if requested, or if cache is empty/stale
    const wantRefresh = url.searchParams.get("refresh") === "1";
    if (wantRefresh || !tanamiCache.data) {
      refreshTanami().catch((e) => console.warn("[tanami] on-demand refresh:", e.message));
    }
    res.writeHead(200);
    return res.end(
      JSON.stringify({
        ok: true,
        refreshing: tanamiRefreshRunning,
        fetchedAt: tanamiCache.fetchedAt ? new Date(tanamiCache.fetchedAt).toISOString() : null,
        projects: tanamiCache.data || [],
      })
    );
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: "Not found. Use GET /news, /tanami, or /health" }));
});

server.listen(PORT, () => console.log(`binayah-news-scraper listening on port ${PORT}`));
