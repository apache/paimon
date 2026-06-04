#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const CONTENT_DIR = path.resolve(__dirname, '../../docs/content');
const OUTPUT_DIR = path.resolve(__dirname, '../docs');
const GENERATED_SRC = path.resolve(__dirname, '/tmp/paimon-docs-hugo-backup/layouts/shortcodes/generated');
const GENERATED_DEST = path.resolve(__dirname, '../generated');

const sidebarTree = {};
const redirects = [];

function walkDir(dir) {
  const entries = fs.readdirSync(dir, {withFileTypes: true});
  const files = [];
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...walkDir(full));
    } else if (entry.name.endsWith('.md')) {
      files.push(full);
    }
  }
  return files;
}

function parseFrontmatter(content) {
  const match = content.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
  if (!match) return {frontmatter: {}, body: content};
  const fmStr = match[1];
  const body = match[2];
  const frontmatter = {};
  for (const line of fmStr.split('\n')) {
    const kv = line.match(/^(\w+)\s*[:=]\s*(.*)$/);
    if (kv) {
      let val = kv[2].trim();
      if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1);
      if (val.startsWith("'") && val.endsWith("'")) val = val.slice(1, -1);
      if (val === 'true') val = true;
      if (val === 'false') val = false;
      if (/^\d+$/.test(val)) val = parseInt(val, 10);
      frontmatter[kv[1]] = val;
    }
  }
  // Parse aliases
  const aliasMatch = fmStr.match(/aliases:\s*\n((?:\s*-\s*.+\n?)+)/);
  if (aliasMatch) {
    frontmatter.aliases = aliasMatch[1]
      .split('\n')
      .map(l => l.replace(/^\s*-\s*/, '').trim())
      .filter(Boolean);
  }
  return {frontmatter, body};
}

function buildNewFrontmatter(fm, isIndex) {
  const lines = ['---'];
  if (fm.title) lines.push(`title: "${fm.title}"`);
  if (fm.bookToc === false) lines.push('hide_table_of_contents: true');
  if (fm.weight !== undefined) lines.push(`sidebar_position: ${fm.weight}`);
  lines.push('---');
  return lines.join('\n');
}

function convertRef(refPath, currentFilePath) {
  let target = refPath.replace(/^\//, '').replace(/\.html$/, '');
  let anchor = '';
  const hashIdx = target.indexOf('#');
  if (hashIdx !== -1) {
    anchor = target.slice(hashIdx);
    target = target.slice(0, hashIdx);
  }
  return `./${target}.md${anchor}`;
}

function convertTabs(body) {
  // Convert {{< tabs "id" >}}...{{< /tabs >}} -> <Tabs>...</Tabs>
  const tabsRegex = /\{\{<\s*tabs\s+"([^"]*?)"\s*>\}\}([\s\S]*?)\{\{<\s*\/tabs\s*>\}\}/g;
  body = body.replace(tabsRegex, (match, id, inner) => {
    // Convert {{< tab "Name" >}}...{{< /tab >}} inside
    const tabRegex = /\{\{<\s*tab\s+"([^"]*?)"\s*>\}\}([\s\S]*?)\{\{<\s*\/tab\s*>\}\}/g;
    let tabItems = '';
    let m;
    while ((m = tabRegex.exec(inner)) !== null) {
      const label = m[1];
      const value = label.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-$/, '');
      const content = m[2].trim();
      tabItems += `<TabItem value="${value}" label="${label}">\n\n${content}\n\n</TabItem>\n\n`;
    }
    return `<Tabs groupId="${id.toLowerCase().replace(/[^a-z0-9]+/g, '-')}">\n\n${tabItems}</Tabs>`;
  });
  return body;
}

function convertHints(body) {
  const hintRegex = /\{\{<\s*hint\s+(info|warning|danger|)\s*>\}\}([\s\S]*?)\{\{<\s*\/hint\s*>\}\}/g;
  body = body.replace(hintRegex, (match, type, content) => {
    const admonType = type || 'info';
    return `:::${admonType}\n\n${content.trim()}\n\n:::`;
  });
  return body;
}

function convertStableUnstable(body) {
  body = body.replace(
    /\{\{<\s*stable\s*>\}\}([\s\S]*?)\{\{<\s*\/stable\s*>\}\}/g,
    '<Stable>\n\n$1\n\n</Stable>'
  );
  body = body.replace(
    /\{\{<\s*unstable\s*>\}\}([\s\S]*?)\{\{<\s*\/unstable\s*>\}\}/g,
    '<Unstable>\n\n$1\n\n</Unstable>'
  );
  return body;
}

function convertImages(body) {
  // {{< img src="/img/foo.png" >}} or with alt/width
  body = body.replace(
    /\{\{<\s*img\s+src="([^"]+)"(?:\s+alt="([^"]*)")?(?:\s+width="([^"]*)")?\s*>\}\}/g,
    (match, src, alt, width) => {
      if (width) {
        return `<img src="${src}" alt="${alt || ''}" style={{width: '${width}'}} />`;
      }
      return `![${alt || ''}](${src})`;
    }
  );
  return body;
}

function convertColumns(body) {
  const colRegex = /\{\{<\s*columns\s*>\}\}([\s\S]*?)\{\{<\s*\/columns\s*>\}\}/g;
  body = body.replace(colRegex, (match, inner) => {
    const parts = inner.split('<--->');
    if (parts.length === 2) {
      return `<div className="columns-wrapper">\n<div>\n\n${parts[0].trim()}\n\n</div>\n<div>\n\n${parts[1].trim()}\n\n</div>\n</div>`;
    }
    return inner;
  });
  return body;
}

function convertGenerated(body) {
  const genRegex = /\{\{<\s*generated\/([a-z_]+)\s*>\}\}/g;
  body = body.replace(genRegex, (match, name) => {
    return `<ConfigTable file="${name}" />`;
  });
  return body;
}

function convertInlineShortcodes(body) {
  // {{< version >}}
  body = body.replace(/\{\{<\s*version\s*>\}\}/g, '@@VERSION@@');
  // {{< param FlinkVersion >}} / {{< param Branch >}} etc
  body = body.replace(/\{\{<\s*param\s+FlinkVersion\s*>\}\}/g, '@@FLINK_VERSION@@');
  body = body.replace(/\{\{<\s*param\s+Branch\s*>\}\}/g, '@@BRANCH@@');
  body = body.replace(/\{\{<\s*param\s+Version\s*>\}\}/g, '@@VERSION@@');
  // {{< github_repo >}}
  body = body.replace(/\{\{<\s*github_repo\s*>\}\}/g, '@@GITHUB_REPO@@');
  body = body.replace(/\{\{<\s*trino_github_repo\s*>\}\}/g, '@@TRINO_GITHUB_REPO@@');
  body = body.replace(/\{\{<\s*presto_github_repo\s*>\}\}/g, '@@PRESTO_GITHUB_REPO@@');
  body = body.replace(/\{\{<\s*scala_version\s*>\}\}/g, '@@SCALA_VERSION@@');
  // {{< label X >}}
  body = body.replace(/\{\{<\s*label\s+([^>]+?)\s*>\}\}/g, '<Label>$1</Label>');
  // {{< all_versions >}}
  body = body.replace(/\{\{<\s*all_versions\s*>\}\}/g, '<AllVersions />');
  // {{< top >}}
  body = body.replace(/\{\{<\s*top\s*>\}\}/g, '');
  // {{< beta >}}...{{< /beta >}}
  body = body.replace(
    /\{\{<\s*beta\s*>\}\}([\s\S]*?)\{\{<\s*\/beta\s*>\}\}/g,
    ':::warning Beta\n\n$1\n\n:::'
  );
  // {{< query_state_warning >}}
  body = body.replace(
    /\{\{<\s*query_state_warning\s*>\}\}/g,
    ':::warning\nQuery state is not supported in streaming mode.\n:::'
  );
  // {{< redoc_rest_catalog_api >}}
  body = body.replace(
    /\{\{<\s*redoc_rest_catalog_api\s*>\}\}/g,
    '<iframe src="/docs/master/rest-catalog-open-api.yaml" width="100%" height="800px" />'
  );
  return body;
}

function convertRefLinks(body, currentFilePath) {
  // [text]({{< ref "path" >}}) or [text]({{< ref "path#anchor" >}})
  body = body.replace(
    /\(\{\{<\s*ref\s+"([^"]+)"\s*>\}\}\)/g,
    (match, refPath) => {
      const converted = convertRef(refPath, currentFilePath);
      return `(${converted})`;
    }
  );
  return body;
}

function convertGhLink(body) {
  // {{< gh_link "path/to/file" >}} -> GitHub link
  body = body.replace(
    /\{\{<\s*gh_link\s+"([^"]+)"\s*>\}\}/g,
    'https://github.com/apache/paimon/blob/@@BRANCH@@/$1'
  );
  return body;
}

function convertJavadoc(body) {
  // {{< javadoc "path" >}} -> javadoc link
  body = body.replace(
    /\{\{<\s*javadoc\s+"([^"]+)"\s*>\}\}/g,
    'https://paimon.apache.org/docs/master/api/java/$1'
  );
  return body;
}

function detectRequiredImports(body) {
  const imports = [];
  if (body.includes('<Tabs') || body.includes('<TabItem')) {
    imports.push("import Tabs from '@theme/Tabs';");
    imports.push("import TabItem from '@theme/TabItem';");
  }
  if (body.includes('<Stable>')) {
    imports.push("import Stable from '@site/src/components/Stable';");
  }
  if (body.includes('<Unstable>')) {
    imports.push("import Unstable from '@site/src/components/Unstable';");
  }
  if (body.includes('<ConfigTable')) {
    imports.push("import ConfigTable from '@site/src/components/ConfigTable';");
  }
  if (body.includes('<Label>')) {
    imports.push("import Label from '@site/src/components/Label';");
  }
  if (body.includes('<AllVersions')) {
    imports.push("import AllVersions from '@site/src/components/AllVersions';");
  }
  return imports;
}

function detectConfigTableImports(body) {
  const imports = [];
  const regex = /<ConfigTable file="([^"]+)"/g;
  let m;
  while ((m = regex.exec(body)) !== null) {
    imports.push(`import ${camelCase(m[1])}Html from '@site/generated/${m[1]}.raw.html';`);
  }
  // Replace <ConfigTable file="xxx" /> with <ConfigTable html={xxxHtml} />
  body = body.replace(
    /<ConfigTable file="([^"]+)"\s*\/>/g,
    (match, name) => `<ConfigTable html={${camelCase(name)}Html} />`
  );
  return {body, imports};
}

function camelCase(str) {
  return str.replace(/_([a-z])/g, (m, c) => c.toUpperCase());
}

function processFile(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const {frontmatter, body: rawBody} = parseFrontmatter(content);
  const relPath = path.relative(CONTENT_DIR, filePath);

  // Collect aliases for redirects
  if (frontmatter.aliases && Array.isArray(frontmatter.aliases)) {
    for (const alias of frontmatter.aliases) {
      redirects.push({from: alias, to: '/' + relPath.replace(/_index\.md$/, '').replace(/\.md$/, '').replace(/\/$/, '')});
    }
  }

  // Collect sidebar info
  const dir = path.dirname(relPath);
  const isIndex = path.basename(filePath) === '_index.md';
  if (!sidebarTree[dir]) sidebarTree[dir] = [];
  sidebarTree[dir].push({
    file: relPath,
    weight: frontmatter.weight || 999,
    title: frontmatter.title || path.basename(filePath, '.md'),
    isIndex,
    hidden: frontmatter.bookHidden === true,
  });

  // Convert body
  let body = rawBody;
  body = convertInlineShortcodes(body);
  body = convertGhLink(body);
  body = convertJavadoc(body);
  body = convertRefLinks(body, filePath);
  body = convertHints(body);
  body = convertStableUnstable(body);
  body = convertTabs(body);
  body = convertImages(body);
  body = convertColumns(body);
  body = convertGenerated(body);

  // Handle config table imports
  const configResult = detectConfigTableImports(body);
  body = configResult.body;
  const configImports = configResult.imports;

  // Detect required imports
  const componentImports = detectRequiredImports(body);
  const allImports = [...componentImports, ...configImports];

  // Build new frontmatter
  const newFm = buildNewFrontmatter(frontmatter, isIndex);

  // Assemble final content
  let finalContent = newFm + '\n';
  if (allImports.length > 0) {
    finalContent += '\n' + allImports.join('\n') + '\n';
  }
  finalContent += '\n' + body;

  // Determine output path
  let outRelPath = relPath;
  if (isIndex) {
    outRelPath = relPath.replace('_index.md', 'index.md');
  }
  // Use .mdx extension if file has JSX
  if (allImports.length > 0 || body.includes('<Tabs') || body.includes('<div className')) {
    outRelPath = outRelPath.replace(/\.md$/, '.mdx');
  }

  const outPath = path.join(OUTPUT_DIR, outRelPath);
  fs.mkdirSync(path.dirname(outPath), {recursive: true});
  fs.writeFileSync(outPath, finalContent);
  console.log(`  Converted: ${relPath} -> ${outRelPath}`);
}

function generateSidebars() {
  // Build sidebar from collected data
  const allFiles = walkDir(CONTENT_DIR);
  // Group by first-level directory
  const sections = {};
  for (const file of allFiles) {
    const rel = path.relative(CONTENT_DIR, file);
    const parts = rel.split(path.sep);
    if (parts.length === 1) continue; // root level files
    const section = parts[0];
    if (!sections[section]) sections[section] = [];
    sections[section].push(file);
  }

  // Read all frontmatter to get ordering
  const sectionMeta = {};
  const itemMeta = {};

  for (const file of allFiles) {
    const content = fs.readFileSync(file, 'utf-8');
    const {frontmatter} = parseFrontmatter(content);
    const rel = path.relative(CONTENT_DIR, file);
    itemMeta[rel] = frontmatter;
  }

  // Get section order from _index.md weights
  const sectionOrder = Object.keys(sections).sort((a, b) => {
    const wa = (itemMeta[`${a}/_index.md`] || {}).weight || 999;
    const wb = (itemMeta[`${b}/_index.md`] || {}).weight || 999;
    return wa - wb;
  });

  function buildCategoryItems(sectionPath) {
    const items = [];
    const dir = path.join(CONTENT_DIR, sectionPath);
    if (!fs.existsSync(dir)) return items;

    const entries = fs.readdirSync(dir, {withFileTypes: true});
    const fileEntries = [];
    const dirEntries = [];

    for (const entry of entries) {
      if (entry.name === '_index.md') continue;
      if (entry.isDirectory()) {
        dirEntries.push(entry.name);
      } else if (entry.name.endsWith('.md')) {
        fileEntries.push(entry.name);
      }
    }

    // Sort files by weight
    fileEntries.sort((a, b) => {
      const relA = path.join(sectionPath, a);
      const relB = path.join(sectionPath, b);
      const wa = (itemMeta[relA] || {}).weight || 999;
      const wb = (itemMeta[relB] || {}).weight || 999;
      return wa - wb;
    });

    for (const f of fileEntries) {
      const rel = path.join(sectionPath, f);
      const fm = itemMeta[rel] || {};
      if (fm.bookHidden) continue;
      let docId = rel.replace(/\.md$/, '').replace(/\\/g, '/');
      // If file was converted to .mdx, docId is still the same (without extension)
      items.push(docId);
    }

    // Sort subdirs by their _index.md weight
    dirEntries.sort((a, b) => {
      const relA = path.join(sectionPath, a, '_index.md');
      const relB = path.join(sectionPath, b, '_index.md');
      const wa = (itemMeta[relA] || {}).weight || 999;
      const wb = (itemMeta[relB] || {}).weight || 999;
      return wa - wb;
    });

    for (const d of dirEntries) {
      const subPath = path.join(sectionPath, d);
      const indexMeta = itemMeta[path.join(subPath, '_index.md')] || {};
      const subItems = buildCategoryItems(subPath);
      items.push({
        type: 'category',
        label: indexMeta.title || d,
        collapsed: true,
        link: {type: 'generated-index'},
        items: subItems,
      });
    }

    return items;
  }

  const sidebar = [];
  for (const section of sectionOrder) {
    const indexMeta = itemMeta[`${section}/_index.md`] || {};
    const items = buildCategoryItems(section);
    sidebar.push({
      type: 'category',
      label: indexMeta.title || section,
      collapsed: true,
      link: {type: 'doc', id: `${section}/index`},
      items,
    });
  }

  const sidebarContent = `// Auto-generated by migration script
/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: ${JSON.stringify(sidebar, null, 2).replace(/"type"/g, 'type').replace(/"category"/g, '"category"')},
};

module.exports = sidebars;
`;
  fs.writeFileSync(path.join(OUTPUT_DIR, '../sidebars.js'), sidebarContent);
  console.log('\n  Generated: sidebars.js');
}

function generateRedirects() {
  const content = `// Auto-generated redirects from Hugo aliases
module.exports = ${JSON.stringify(redirects, null, 2)};
`;
  fs.writeFileSync(path.join(OUTPUT_DIR, '../redirects.js'), content);
  console.log(`  Generated: redirects.js (${redirects.length} redirects)`);
}

function copyGeneratedConfigs() {
  if (!fs.existsSync(GENERATED_SRC)) {
    console.log('  Warning: Generated config source not found, skipping');
    return;
  }
  fs.mkdirSync(GENERATED_DEST, {recursive: true});
  const files = fs.readdirSync(GENERATED_SRC).filter(f => f.endsWith('.html'));
  for (const file of files) {
    let content = fs.readFileSync(path.join(GENERATED_SRC, file), 'utf-8');
    // Remove Hugo template comments {{/* ... */}}
    content = content.replace(/\{\{\/\*[\s\S]*?\*\/\}\}\n?/g, '');
    const outName = file.replace('.html', '.raw.html');
    fs.writeFileSync(path.join(GENERATED_DEST, outName), content);
  }
  console.log(`  Copied ${files.length} generated config tables to generated/`);
}

// Main
console.log('Starting Hugo -> Docusaurus migration...\n');
console.log('Phase 1: Converting markdown files...');
const allFiles = walkDir(CONTENT_DIR);
console.log(`  Found ${allFiles.length} markdown files\n`);

for (const file of allFiles) {
  processFile(file);
}

console.log('\nPhase 2: Generating sidebars...');
generateSidebars();

console.log('\nPhase 3: Generating redirects...');
generateRedirects();

console.log('\nPhase 4: Copying generated config tables...');
copyGeneratedConfigs();

console.log('\nMigration complete!');
