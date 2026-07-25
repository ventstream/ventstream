# VentStream docs site

Mintlify documentation site for VentStream. Same platform as the
Wundergraph Cosmo docs.

## Local preview

Use Node.js 20 LTS; the Mint CLI does not support Node.js 25 or newer.

```bash
npm i -g mint        # one-time
cd docs-site
mint dev             # → http://localhost:3000 (or --port)
```

## Validate

```bash
mint validate        # MDX/navigation build validation
mint broken-links    # documentation link scan
```

## Structure

- `docs.json` — navigation, theme, colors, navbar. Edit this to add
  pages or reorder groups.
- `*.mdx` — one file per page. Frontmatter (`title`, `description`) is
  required.
- `images/` — favicon + any assets.

Page groups: **Get started**, **Concepts**, **Connectors**, **Guides**,
**Fleet management**, **Deploy**, **Reference**, **Operations**.

## Deploy

Not hosted yet — read it locally with `mint dev` (above). Mintlify is
hosted-only (no static export / self-host); when we publish, it'll be by
connecting this repo in the Mintlify dashboard. The site is the source of
truth for public product docs. Files under `docs/` are engineering contracts,
test guides, and release evidence; `docs/deployment.html` only redirects readers
to the maintained deployment pages here.
