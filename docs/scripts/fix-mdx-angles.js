#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const docsRoot = path.resolve(__dirname, '../docs');

// Find all .mdx files
function findMdx(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) results.push(...findMdx(full));
    else if (entry.name.endsWith('.mdx')) results.push(full);
  }
  return results;
}

// Valid JSX/HTML tags that should NOT be escaped
const validTags = new Set([
  'Tabs', 'TabItem', 'Stable', 'Unstable', 'ConfigTable', 'Label', 'AllVersions',
  'div', 'span', 'img', 'a', 'br', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
  'table', 'thead', 'tbody', 'tr', 'td', 'th', 'ul', 'ol', 'li',
  'strong', 'em', 'code', 'pre', 'blockquote', 'hr', 'sup', 'sub',
  'details', 'summary', 'iframe',
]);

function isValidTag(tag) {
  // Check opening or self-closing
  const name = tag.replace(/^\//, '').split(/[\s/]/)[0];
  return validTags.has(name);
}

function processFile(filePath) {
  let content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n');
  let inCodeFence = false;
  let modified = false;

  const processed = lines.map(line => {
    // Track code fences
    if (line.match(/^```/)) {
      inCodeFence = !inCodeFence;
      return line;
    }
    if (inCodeFence) return line;

    // Skip import lines
    if (line.startsWith('import ')) return line;

    // Skip lines that are pure JSX components (start with < and a valid tag)
    if (line.trim().startsWith('<') && line.trim().match(/^<\/?(Tabs|TabItem|Stable|Unstable|ConfigTable|Label|AllVersions|div|img|iframe)/)) {
      return line;
    }

    // Process the line: escape < that don't start valid tags
    let result = '';
    let inInlineCode = false;
    let i = 0;
    while (i < line.length) {
      if (line[i] === '`') {
        inInlineCode = !inInlineCode;
        result += line[i];
        i++;
        continue;
      }
      if (inInlineCode) {
        result += line[i];
        i++;
        continue;
      }
      if (line[i] === '<') {
        // Look ahead to determine if this is a valid tag
        const rest = line.slice(i + 1);
        const tagMatch = rest.match(/^(\/?\w[\w-]*)/);
        if (tagMatch) {
          const tagName = tagMatch[1];
          if (isValidTag(tagName)) {
            result += line[i];
            i++;
            continue;
          }
        }
        // Check if it's an HTML comment
        if (rest.startsWith('!--')) {
          result += line[i];
          i++;
          continue;
        }
        // Check if it's inside a JSX expression {<...>}
        // This is a non-valid tag, escape it
        result += '\\<';
        modified = true;
        i++;
        continue;
      }
      // Also handle bare { that could be parsed as JSX expressions in certain contexts
      result += line[i];
      i++;
    }
    return result;
  });

  if (modified) {
    fs.writeFileSync(filePath, processed.join('\n'));
    console.log(`  Fixed: ${path.relative(docsRoot, filePath)}`);
  }
}

const files = findMdx(docsRoot);
console.log(`Processing ${files.length} .mdx files...`);
for (const file of files) {
  processFile(file);
}
console.log('Done!');
