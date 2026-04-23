// Vercel serverless proxy for Google News batchexecute URL resolution.
// Render's IPs get 429 from Google; Vercel's IP pool is not affected.
export const config = { runtime: "nodejs" };

const CHROME_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

async function getDecodingParams(articleId) {
  for (const base of [
    `https://news.google.com/articles/${articleId}`,
    `https://news.google.com/rss/articles/${articleId}`,
  ]) {
    try {
      const res = await fetch(base, {
        headers: { "User-Agent": CHROME_UA },
        redirect: "follow",
      });
      if (!res.ok) continue;
      const html = await res.text();
      // Extract data-n-a-sg and data-n-a-ts from the c-wiz jscontroller div
      const sgMatch = html.match(/data-n-a-sg="([^"]+)"/);
      const tsMatch = html.match(/data-n-a-ts="([^"]+)"/);
      if (sgMatch && tsMatch) {
        return { signature: sgMatch[1], timestamp: tsMatch[1] };
      }
    } catch (_) {
      // try next base
    }
  }
  return null;
}

async function batchExecute(articleId, timestamp, signature) {
  const payload = JSON.stringify([
    [
      [
        "Fbv4je",
        `["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,null,null,null,null,null,0,1],"X","X",1,[1,1,1],1,1,null,0,0,null,0],"${articleId}",${timestamp},"${signature}"]`,
        null,
        "generic",
      ],
    ],
  ]);

  const body = `f.req=${encodeURIComponent(payload)}`;

  const res = await fetch(
    "https://news.google.com/_/DotsSplashUi/data/batchexecute",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "User-Agent": CHROME_UA,
        Referer: `https://news.google.com/articles/${articleId}`,
      },
      body,
    }
  );

  if (!res.ok) throw new Error(`batchexecute HTTP ${res.status}`);

  const text = await res.text();
  const parts = text.split("\n\n");
  if (parts.length < 2) throw new Error("unexpected batchexecute response format");

  const parsed = JSON.parse(parts[1]);
  const inner = JSON.parse(parsed[0][2]);
  return inner[1];
}

export default async function handler(req, res) {
  const { id } = req.query || {};

  if (!id || typeof id !== "string") {
    return res.status(400).json({ error: "missing ?id= param" });
  }

  try {
    const params = await getDecodingParams(id);
    if (!params) {
      return res.status(502).json({ error: "could not extract sig/ts from Google News page" });
    }

    const url = await batchExecute(id, params.timestamp, params.signature);
    res.setHeader("Cache-Control", "public, s-maxage=86400, stale-while-revalidate=3600");
    return res.status(200).json({ url });
  } catch (err) {
    return res.status(502).json({ error: err.message });
  }
}
