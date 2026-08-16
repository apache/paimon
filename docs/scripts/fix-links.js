#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const docsRoot = path.resolve(__dirname, '../docs');

function findFiles(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) results.push(...findFiles(full));
    else if (entry.name.endsWith('.md') || entry.name.endsWith('.mdx')) results.push(full);
  }
  return results;
}

function fixLinks(filePath) {
  let content = fs.readFileSync(filePath, 'utf-8');
  const fileDir = path.dirname(path.relative(docsRoot, filePath));
  let modified = false;

  // Fix links like [text](./section/page.md) that should be [text](../section/page.md)
  // The pattern: a link starting with ./ that references a top-level section different from current
  content = content.replace(/\]\(\.\/([a-z][\w-]*\/[^\)]*)\)/g, (match, linkPath) => {
    // Extract the target section (first path segment)
    const targetSection = linkPath.split('/')[0];
    const currentSection = fileDir.split('/')[0];

    // If targeting a different section, need to go up
    if (targetSection !== currentSection && targetSection !== '.' && targetSection !== '') {
      // Calculate how many levels up we need to go
      const depth = fileDir.split('/').length;
      const prefix = '../'.repeat(depth);
      modified = true;
      return `](${prefix}${linkPath})`;
    }

    // If same section but in a subdirectory, the ./ is correct
    return match;
  });

  // Also fix links like [text](./page.md) within same directory that reference other sections
  // These are links like ./concepts/rest/overview.md from within concepts/rest/ -> should be ./overview.md
  // But more commonly they are cross-section links from a page like concepts/catalog.md linking to ./concepts/rest/overview.md
  // which should be ./rest/overview.md (since we're already in concepts/)

  // Fix self-referencing section prefix: ./concepts/rest/rest-api.md from within concepts/rest/
  content = content.replace(/\]\(\.\/([\w-]+)\/([\w-]+\/[^\)]*)\)/g, (match, firstSeg, rest) => {
    const currentSection = fileDir.split('/')[0];
    const currentSubdir = fileDir.split('/')[1] || '';

    if (firstSeg === currentSection) {
      // We're in the same section, remove the redundant prefix
      modified = true;
      return `](./${rest})`;
    }
    return match;
  });

  // Fix absolute-style links starting with ./ from root-level pages
  // e.g., from concepts/overview.md, a link ./concepts/rest/overview.md should be ./rest/overview.md

  if (modified) {
    fs.writeFileSync(filePath, content);
    console.log(`  Fixed: ${path.relative(docsRoot, filePath)}`);
  }
}

// A more thorough approach: recompute all ref links properly
function fixAllLinks(filePath) {
  let content = fs.readFileSync(filePath, 'utf-8');
  const relFile = path.relative(docsRoot, filePath);
  const fileDir = path.dirname(relFile); // e.g., "concepts" or "concepts/rest"
  let modified = false;

  // Match markdown links: [text](./path/to/file.md) or [text](./path/to/file.md#anchor)
  content = content.replace(/\]\(\.\/([\w-]+(?:\/[\w-.#]+)*(?:\.mdx?)?(?:#[\w-]*)?)\)/g, (match, rawLink) => {
    // Split anchor
    let anchor = '';
    let linkPath = rawLink;
    const hashIdx = linkPath.indexOf('#');
    if (hashIdx !== -1) {
      anchor = linkPath.slice(hashIdx);
      linkPath = linkPath.slice(0, hashIdx);
    }

    // If the link doesn't have a / it's a same-directory link, leave it
    if (!linkPath.includes('/')) return match;

    // Resolve what the link is trying to point to (assuming it was meant as absolute from docs root)
    // Check if the resolved target exists
    const resolvedFromCurrent = path.join(docsRoot, fileDir, linkPath);
    if (fs.existsSync(resolvedFromCurrent) || fs.existsSync(resolvedFromCurrent.replace(/\.md$/, '.mdx'))) {
      // Link resolves correctly from current dir, no fix needed
      return match;
    }

    // Try resolving from docs root
    const resolvedFromRoot = path.join(docsRoot, linkPath);
    const resolvedFromRootMdx = resolvedFromRoot.replace(/\.md$/, '.mdx');
    if (fs.existsSync(resolvedFromRoot) || fs.existsSync(resolvedFromRootMdx)) {
      // The link target exists at root level, compute relative path from current file
      const relativePath = path.relative(fileDir, linkPath);
      modified = true;
      return `](./${relativePath}${anchor})`;
    }

    // Try without extension
    const noExt = linkPath.replace(/\.mdx?$/, '');
    const possibles = [
      path.join(docsRoot, noExt + '.md'),
      path.join(docsRoot, noExt + '.mdx'),
      path.join(docsRoot, noExt, 'index.md'),
      path.join(docsRoot, noExt, 'index.mdx'),
    ];
    for (const p of possibles) {
      if (fs.existsSync(p)) {
        const targetRel = path.relative(docsRoot, p);
        const targetDir = path.dirname(targetRel);
        const relativePath = path.relative(fileDir, targetRel);
        modified = true;
        return `](./${relativePath}${anchor})`;
      }
    }

    // Can't resolve, leave as-is
    return match;
  });

  if (modified) {
    fs.writeFileSync(filePath, content);
    console.log(`  Fixed: ${relFile}`);
  }
}

const files = findFiles(docsRoot);
console.log(`Fixing links in ${files.length} files...`);
for (const file of files) {
  fixAllLinks(file);
}
console.log('Done!');
