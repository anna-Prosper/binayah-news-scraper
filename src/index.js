import http from "http";
import RssParser from "rss-parser";

const PORT = process.env.PORT || 3001;
const CACHE_TTL_MS = 30 * 60 * 1000; // 30 min

const UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
const parser = new RssParser({ timeout: 12_000, headers: { "User-Agent": UA } });

// ── RSS sources ──────────────────────────────────────────────────────────────

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
];

function gnewsUrl(q) {
  return `https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=en&gl=AE&ceid=AE:en`;
}

// ── HTML scrapers ────────────────────────────────────────────────────────────

async function scrapeGulfNews() {
  const res = await fetch("https://gulfnews.com/business/property", {
    headers: { "User-Agent": UA, Accept: "text/html" },
    signal: AbortSignal.timeout(15_000),
  });
  if (!res.ok) throw new Error(`Gulf News HTTP ${res.status}`);
  const html = await res.text();

  const decode = (s) =>
    s.replace(/&amp;/g, "&").replace(/&#x27;/g, "'")
     .replace(/&quot;/g, '"').replace(/&lt;/g, "<").replace(/&gt;/g, ">");

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
    articles.push({
      source: "Gulf News", title, url, summary,
      imageUrl: img ? "https:" + img[1].split("?")[0] : "",
      publishedAt: new Date().toISOString(),
    });
  }
  return articles;
}

async function scrapeKhaleejTimes() {
  const res = await fetch("https://www.khaleejtimes.com/business/property", {
    headers: { "User-Agent": UA, Accept: "text/html" },
    signal: AbortSignal.timeout(15_000),
  });
  if (!res.ok) throw new Error(`Khaleej Times HTTP ${res.status}`);

  const html = (await res.text())
    .replace(/&quot;/g, '"').replace(/&amp;/g, "&")
    .replace(/&#x27;/g, "'").replace(/&lt;/g, "<").replace(/&gt;/g, ">");

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

  const nearest = (arr, pos) =>
    arr.filter((x) => x.pos > pos).sort((a, b) => a.pos - b.pos)[0]?.val;

  const articles = [];
  const seen = new Set();
  for (const h of headlines) {
    const url = nearest(urls, h.pos);
    if (!url || seen.has(url)) continue;
    seen.add(url);
    const ts = nearest(timestamps, h.pos);
    articles.push({
      source: "Khaleej Times",
      title: h.val,
      url,
      summary: "",
      imageUrl: (nearest(images, h.pos) ?? "").split("?")[0],
      publishedAt: ts ? new Date(ts).toISOString() : new Date().toISOString(),
    });
  }
  return articles;
}

// ── Fetch all sources ────────────────────────────────────────────────────────

async function fetchAll() {
  const results = [];

  // Direct RSS
  await Promise.allSettled(
    RSS_FEEDS.map(async ({ url, source }) => {
      try {
        const feed = await parser.parseURL(url);
        for (const item of feed.items ?? []) {
          if (!item.title || !item.link) continue;
          results.push({
            source,
            title: item.title.trim(),
            url: item.link,
            summary: (item.contentSnippet || item.summary || "").slice(0, 300),
            imageUrl: (item).enclosure?.url || (item)["media:content"]?.["$"]?.url || "",
            publishedAt: item.pubDate ? new Date(item.pubDate).toISOString() : new Date().toISOString(),
          });
        }
        console.log(`[rss] ${source}: ${feed.items?.length ?? 0} items`);
      } catch (e) {
        console.warn(`[rss] ${source} failed:`, e.message);
      }
    })
  );

  // Google News RSS
  await Promise.allSettled(
    GNEWS_FEEDS.map(async ({ q, source }) => {
      try {
        const feed = await parser.parseURL(gnewsUrl(q));
        for (const item of feed.items ?? []) {
          if (!item.title || !item.link) continue;
          results.push({
            source,
            title: item.title.trim(),
            url: item.link,
            summary: "",
            imageUrl: "",
            publishedAt: item.pubDate ? new Date(item.pubDate).toISOString() : new Date().toISOString(),
          });
        }
        console.log(`[gnews] ${source}: ${feed.items?.length ?? 0} items`);
      } catch (e) {
        console.warn(`[gnews] ${source} failed:`, e.message);
      }
    })
  );

  // HTML scrapers
  for (const [fn, label] of [[scrapeGulfNews, "Gulf News"], [scrapeKhaleejTimes, "Khaleej Times"]]) {
    try {
      const articles = await fn();
      results.push(...articles);
      console.log(`[scrape] ${label}: ${articles.length} articles`);
    } catch (e) {
      console.warn(`[scrape] ${label} failed:`, e.message);
    }
  }

  const RE_KEYWORDS = /real.?estate|property|properties|mortgage|rent|landlord|tenant|apartment|villa|residential|commercial|off.?plan|handover|developer|realty|housing|sqft|sq\.ft|dubai land|DLD|RERA|leasehold|freehold/i;
  const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;
  const cutoff = Date.now() - ONE_WEEK_MS;

  // Filter to real estate articles published within the last 7 days, dedupe by URL
  const seen = new Set();
  return results
    .filter((a) => {
      if (seen.has(a.url)) return false;
      seen.add(a.url);
      const age = new Date(a.publishedAt).getTime();
      if (age < cutoff) return false;
      return RE_KEYWORDS.test(a.title) || RE_KEYWORDS.test(a.summary || "");
    })
    .sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
}

// ── In-memory cache ──────────────────────────────────────────────────────────

let cache = { data: null, fetchedAt: 0 };

async function getNews(limit = 50) {
  if (!cache.data || Date.now() - cache.fetchedAt > CACHE_TTL_MS) {
    console.log("[cache] refreshing...");
    cache.data = await fetchAll();
    cache.fetchedAt = Date.now();
    console.log(`[cache] ${cache.data.length} articles cached`);
  }
  return cache.data.slice(0, limit);
}

// Warm cache on startup
getNews().catch((e) => console.warn("Startup cache warm failed:", e.message));

// ── HTTP server ──────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost`);

  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Content-Type", "application/json");

  if (req.method === "GET" && url.pathname === "/health") {
    res.writeHead(200);
    return res.end(JSON.stringify({ ok: true, cached: !!cache.data, articles: cache.data?.length ?? 0 }));
  }

  if (req.method === "GET" && url.pathname === "/news") {
    try {
      const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "50"), 1000);
      const source = url.searchParams.get("source");
      let articles = await getNews(200);
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
