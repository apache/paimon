# Apache Paimon Documentation

This directory contains the source for the Apache Paimon documentation site, built with [Docusaurus 3](https://docusaurus.io/).

## Prerequisites

- Node.js >= 18
- Yarn (`npm install -g yarn`)

## Quick Start

```bash
# Install dependencies
yarn install

# Start development server (with hot reload)
yarn start
```

The site will be available at http://localhost:3000/docs/master/.

## Build

```bash
# Production build
yarn build

# Preview the production build locally
yarn serve
```

## Project Structure

```
docs/
├── docs/              # Markdown/MDX content files
├── generated/         # Auto-generated config tables (from paimon-docs module)
├── src/
│   ├── components/    # Custom React components (Stable, Unstable, ConfigTable, etc.)
│   ├── css/           # Custom styles
│   └── plugins/       # Remark plugins (variable interpolation)
├── static/            # Static assets (images, logos, OpenAPI spec)
├── scripts/           # Migration utilities
├── docusaurus.config.js
├── sidebars.js
└── package.json
```

## Regenerating Config Tables

The 28 HTML config tables in `generated/` are produced by the `paimon-docs` Maven module. To regenerate:

```bash
cd .. && mvn -pl paimon-docs -am package -DskipTests
```

## Adding a New Page

1. Create a `.md` (or `.mdx` if you need React components like Tabs) file under `docs/`
2. Add frontmatter:
   ```yaml
   ---
   title: "Page Title"
   sidebar_position: 5
   ---
   ```
3. The page will automatically appear in the sidebar based on `sidebar_position`

## Using Tabs

```mdx
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="engine">
<TabItem value="flink" label="Flink">

Flink content here.

</TabItem>
<TabItem value="spark" label="Spark">

Spark content here.

</TabItem>
</Tabs>
```

## Version Variables

Use `@@VERSION@@` in markdown and it will be replaced with the current version (`1.5-SNAPSHOT`) at build time. Other available tokens:

- `@@VERSION@@` — Paimon version
- `@@FLINK_VERSION@@` — Flink version
- `@@BRANCH@@` — Git branch
- `@@GITHUB_REPO@@` — GitHub repo URL
