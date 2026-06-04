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

"""Check that all Python source files contain a valid Apache License header."""

import os
import sys

# Required phrases that must appear in every license header.
# Keep phrases short enough to not be split by narrow line-wrapping.
REQUIRED_PHRASES = [
    'Licensed to the Apache Software Foundation (ASF) under one',
    'you may not use this file except in compliance',
    'http://www.apache.org/licenses/LICENSE-2.0',
    'WITHOUT WARRANTIES OR CONDITIONS OF ANY',
]

SKIP_DIRS = {'.venv', '.venv-py310', '.venv-py311', '.venv-py36',
             '.venv-py36-compat', '.venv-py39', '.venv-py313',
             '__pycache__', 'sample', '.tox', 'node_modules'}


def check_file(filepath):
    """Return a list of issues found in the file's license header."""
    issues = []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
    except Exception as e:
        return [str(e)]

    if not content.strip():
        return []  # skip empty files

    head = content[:2000]

    for phrase in REQUIRED_PHRASES:
        if phrase not in head:
            issues.append('missing: "%s"' % phrase)

    lines = content.split('\n')
    for i, line in enumerate(lines[:3]):
        stripped = line.lstrip('#').lstrip().lstrip('"').lstrip("'").strip()
        if i > 0 and stripped == lines[0].lstrip('#').lstrip().lstrip('"').lstrip("'").strip() \
                and 'Licensed to the Apache' in stripped:
            issues.append('line %d: duplicated license first line' % (i + 1))

    return issues


def main():
    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'pypaimon')
    root = os.path.normpath(root)

    failed = []
    checked = 0

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            if not fname.endswith('.py'):
                continue
            filepath = os.path.join(dirpath, fname)
            checked += 1
            issues = check_file(filepath)
            if issues:
                rel = os.path.relpath(filepath, os.path.join(root, '..'))
                failed.append((rel, issues))

    if failed:
        print('License header issues found in %d file(s):' % len(failed))
        for path, issues in sorted(failed):
            for issue in issues:
                print('  %s: %s' % (path, issue))
        print('\nChecked %d files, %d failed.' % (checked, len(failed)))
        return 1
    else:
        print('All %d files have valid license headers.' % checked)
        return 0


if __name__ == '__main__':
    sys.exit(main())
