#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""One-shot script to unify all .py license headers to the standard style."""

import os
import re
import sys

STANDARD_HEADER = """\
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""

SKIP_DIRS = {'.venv', '.venv-py310', '.venv-py311', '.venv-py36',
             '.venv-py36-compat', '.venv-py39', '.venv-py313',
             '__pycache__', 'sample', '.tox'}

LICENSE_MARKER = 'Licensed to the Apache Software Foundation'


def find_header_end(lines):
    """Find the line index where the license header ends.

    Returns the index of the first line AFTER the header block
    (including any trailing blank line).
    """
    if not lines:
        return 0

    i = 0

    # Skip shebang
    if lines[i].startswith('#!'):
        i += 1
        # skip blank lines after shebang
        while i < len(lines) and lines[i].strip() == '':
            i += 1

    # Detect header style
    first = lines[i].strip() if i < len(lines) else ''

    if first.startswith('##') and '#' * 10 in first:
        # Bordered style: ####...
        # Find closing border
        i += 1
        while i < len(lines):
            if lines[i].strip().startswith('##') and '#' * 10 in lines[i].strip():
                i += 1  # include closing border
                break
            i += 1
    elif first.startswith('#') and LICENSE_MARKER in first:
        # Hash comment style without border
        while i < len(lines) and lines[i].startswith('#'):
            i += 1
    elif first.startswith('"""') or first.startswith("'''"):
        # Docstring style
        if LICENSE_MARKER in ''.join(lines[i:i+3]):
            # Find closing quotes
            quote = first[:3]
            if first.endswith(quote) and len(first) > 3:
                i += 1  # single-line docstring
            else:
                i += 1
                while i < len(lines):
                    if quote in lines[i]:
                        i += 1
                        break
                    i += 1
    else:
        return 0  # no header found

    # Skip blank lines after header
    while i < len(lines) and lines[i].strip() == '':
        i += 1

    return i


def fix_file(filepath, dry_run=False):
    """Fix the license header in a single file. Returns True if changed."""
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()

    if not content.strip():
        return False  # skip empty files

    lines = content.split('\n')

    # Check if file has the license marker anywhere in first 2000 chars
    head = content[:2000]
    has_license = LICENSE_MARKER in head

    # Detect shebang
    shebang = ''
    start = 0
    if lines[0].startswith('#!'):
        shebang = lines[0] + '\n'
        start = 1

    if has_license:
        # Replace existing header
        header_end = find_header_end(lines)
        rest_lines = lines[header_end:]
    else:
        # No header - insert one
        rest_lines = lines[start:]
        # Strip leading blank lines
        while rest_lines and rest_lines[0].strip() == '':
            rest_lines = rest_lines[1:]

    # Check if already correct
    expected_start = STANDARD_HEADER.split('\n')
    actual_start = lines[start:start + len(expected_start)]
    if actual_start == expected_start:
        return False  # already correct

    # Build new content
    new_content = shebang + STANDARD_HEADER + '\n' + '\n'.join(rest_lines)

    # Ensure file ends with exactly one newline
    new_content = new_content.rstrip('\n') + '\n'

    if dry_run:
        return True

    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        f.write(new_content)

    return True


def main():
    dry_run = '--dry-run' in sys.argv
    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'pypaimon')
    root = os.path.normpath(root)

    changed = []
    skipped_no_license = []
    total = 0

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            if not fname.endswith('.py'):
                continue
            filepath = os.path.join(dirpath, fname)
            total += 1

            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            if not content.strip():
                continue

            if LICENSE_MARKER not in content[:2000]:
                rel = os.path.relpath(filepath, os.path.join(root, '..'))
                skipped_no_license.append(rel)
                continue

            if fix_file(filepath, dry_run=dry_run):
                rel = os.path.relpath(filepath, os.path.join(root, '..'))
                changed.append(rel)

    action = 'Would fix' if dry_run else 'Fixed'
    if changed:
        print('%s %d file(s):' % (action, len(changed)))
        for p in sorted(changed):
            print('  %s' % p)
    else:
        print('All files already have the correct header.')

    if skipped_no_license:
        print('\nSkipped %d file(s) with no license (need manual review):' % len(skipped_no_license))
        for p in sorted(skipped_no_license):
            print('  %s' % p)

    print('\nTotal files scanned: %d' % total)
    return 0


if __name__ == '__main__':
    sys.exit(main())
