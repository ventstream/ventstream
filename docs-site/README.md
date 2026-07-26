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

The local server hosts documentation at `/`. Mintlify applies the `/docs` base
path only to hosted preview and production deployments.

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

## Deploy at `ventstream.dev/docs`

The site is rendered by Mintlify and reverse-proxied through the VentStream
Vercel project so public URLs remain under `https://ventstream.dev/docs`.

1. Connect the `ventstream/ventstream` repository in Mintlify.
2. Enable the monorepo setting and use `/docs-site` as the documentation path.
3. Enable **Host at `/docs`** in Mintlify domain settings and add
   `ventstream.dev`.
4. Set the resulting Mintlify project identifier as `MINTLIFY_SUBDOMAIN` in the
   `ventstream-web` Vercel project. The Vercel rewrite targets
   `https://<identifier>.mintlify.site/docs`.
5. Deploy the website and verify `/docs`, `/docs/quickstart`,
   `/docs/sitemap.xml`, and `/docs/llms.txt`.

This directory is the source of truth for public product documentation. Files
under `docs/` are engineering contracts, test guides, and release evidence;
`docs/deployment.html` only redirects readers to the maintained deployment pages
here.
