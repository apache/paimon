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

function fileExists(p) {
  if (fs.existsSync(p)) return true;
  if (fs.existsSync(p.replace(/\.md$/, '.mdx'))) return true;
  if (fs.existsSync(p.replace(/\.mdx?$/, '') + '/index.md')) return true;
  if (fs.existsSync(p.replace(/\.mdx?$/, '') + '/index.mdx')) return true;
  return false;
}

function fixFile(filePath) {
  let content = fs.readFileSync(filePath, 'utf-8');
  const relFile = path.relative(docsRoot, filePath);
  const fileDir = path.dirname(relFile);
  let modified = false;

  // Match all markdown links with relative paths
  content = content.replace(/\]\((\.\.?\/[^\)]+)\)/g, (match, rawLink) => {
    // Split anchor
    let anchor = '';
    let linkPath = rawLink;
    const hashIdx = linkPath.indexOf('#');
    if (hashIdx !== -1) {
      anchor = linkPath.slice(hashIdx);
      linkPath = linkPath.slice(0, hashIdx);
    }

    // Skip external/absolute links
    if (linkPath.startsWith('http') || linkPath.startsWith('/docs/master/api')) return match;

    // Resolve the link relative to current file dir
    const resolved = path.resolve(path.join(docsRoot, fileDir), linkPath);
    const resolvedRel = path.relative(docsRoot, resolved);

    // Check if it actually exists
    if (fileExists(resolved)) return match;

    // It doesn't exist. Try to find it from docs root
    // Extract the "intended" path (strip leading ../ and ./)
    const strippedPath = linkPath.replace(/^(\.\.\/)+/, '').replace(/^\.\//, '');

    // Try resolving from docs root
    const fromRoot = path.join(docsRoot, strippedPath);
    if (fileExists(fromRoot)) {
      // Compute correct relative path from current file
      const targetFile = fs.existsSync(fromRoot) ? fromRoot :
        fs.existsSync(fromRoot.replace(/\.md$/, '.mdx')) ? fromRoot.replace(/\.md$/, '.mdx') :
        fs.existsSync(fromRoot.replace(/\.mdx?$/, '') + '/index.md') ? fromRoot.replace(/\.mdx?$/, '') + '/index.md' :
        fromRoot;
      const correctRel = path.relative(path.join(docsRoot, fileDir), fromRoot);
      const newLink = correctRel.startsWith('.') ? correctRel : './' + correctRel;
      modified = true;
      return `](${newLink}${anchor})`;
    }

    // Leave as-is
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
  fixFile(file);
}
console.log('Done!');
