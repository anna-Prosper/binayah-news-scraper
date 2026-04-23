// Vercel proxy: fetch og:image from a page URL — for domains that block Render IPs
export const config = { runtime: "nodejs" };

const CHROME_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

export default async function handler(req, res) {
  const { url } = req.query || {};
  if (!url || !url.startsWith("http")) {
    return res.status(400).json({ error: "missing or invalid ?url= param" });
  }

  try {
    const r = await fetch(url, {
      headers: {
        "User-Agent": CHROME_UA,
        Accept: "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(12_000),
    });

    if (!r.ok) return res.status(502).json({ error: `upstream ${r.status}` });

    const html = await r.text();

    const patterns = [
      /property=["']og:image["'][^>]+content=["']([^"']+)["']/i,
      /content=["']([^"']+)["'][^>]+property=["']og:image["']/i,
      /name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i,
      /content=["']([^"']+)["'][^>]+name=["']twitter:image["']/i,
    ];

    let imageUrl = "";
    for (const pat of patterns) {
      const m = html.match(pat);
      if (m && m[1].startsWith("http")) { imageUrl = m[1]; break; }
    }

    res.setHeader("Cache-Control", "public, s-maxage=3600, stale-while-revalidate=600");
    return res.status(200).json({ imageUrl });
  } catch (err) {
    return res.status(502).json({ error: err.message });
  }
}
