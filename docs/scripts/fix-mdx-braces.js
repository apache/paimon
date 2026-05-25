#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const docsRoot = path.resolve(__dirname, '../docs');

function findMdx(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) results.push(...findMdx(full));
    else if (entry.name.endsWith('.mdx')) results.push(full);
  }
  return results;
}

function processFile(filePath) {
  let content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n');
  let inCodeFence = false;
  let modified = false;

  const processed = lines.map(line => {
    if (line.match(/^```/)) {
      inCodeFence = !inCodeFence;
      return line;
    }
    if (inCodeFence) return line;
    if (line.startsWith('import ')) return line;

    // Skip lines that are JSX component usage (contain JSX attributes with {})
    if (line.match(/^\s*<(Tabs|TabItem|Stable|Unstable|ConfigTable|Label|AllVersions|div|img)/)) return line;
    if (line.match(/html=\{/)) return line;
    if (line.match(/groupId="/)) return line;
    if (line.match(/style=\{\{/)) return line;
    if (line.match(/className="/)) return line;

    // Escape { and } that are NOT part of JSX expressions
    // Only escape if the line contains { but is clearly not JSX
    if (line.includes('{') || line.includes('}')) {
      // Check if this is a JSX expression line (starts with or is part of a component)
      if (line.match(/^\s*<\/?(Tabs|TabItem|Stable|Unstable|ConfigTable|Label|AllVersions)/)) return line;

      // Escape curlies in lines that have them in regular text/HTML content
      let result = '';
      let inInlineCode = false;
      for (let i = 0; i < line.length; i++) {
        if (line[i] === '`') {
          inInlineCode = !inInlineCode;
          result += line[i];
          continue;
        }
        if (inInlineCode) {
          result += line[i];
          continue;
        }
        if (line[i] === '{') {
          // Check if this is a JSX expression we should keep
          // Keep: {xxxHtml}, {children}, {'<'}, style={{...}}
          const rest = line.slice(i);
          if (rest.match(/^\{[a-zA-Z]+Html\}/)) {
            result += line[i];
            continue;
          }
          if (rest.match(/^\{\{/)) {
            // style={{ }}, keep double curlies
            result += line[i];
            continue;
          }
          if (rest.match(/^\{'[<>{}]'\}/)) {
            result += line[i];
            continue;
          }
          // This is a text curly brace, escape it
          result += "{'\\{'}";
          modified = true;
          continue;
        }
        if (line[i] === '}') {
          // Check if this is closing a JSX expression we kept
          const before = result;
          if (before.match(/\{[a-zA-Z]+Html$/)) {
            result += line[i];
            continue;
          }
          if (before.match(/\{\{[^}]*$/)) {
            result += line[i];
            continue;
          }
          if (before.match(/\{'[<>{}]'$/)) {
            result += line[i];
            continue;
          }
          if (before.match(/\{'\\{'\}?$/)) {
            // Already escaped opening brace
            result += line[i];
            continue;
          }
          // This is a text curly brace, escape it
          result += "{'\\}'}";
          modified = true;
          continue;
        }
        result += line[i];
      }
      return result;
    }
    return line;
  });

  if (modified) {
    fs.writeFileSync(filePath, processed.join('\n'));
    console.log(`  Fixed braces: ${path.relative(docsRoot, filePath)}`);
  }
}

const files = findMdx(docsRoot);
console.log(`Processing ${files.length} .mdx files for brace escaping...`);
for (const file of files) {
  processFile(file);
}
console.log('Done!');
