# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Partition the collected pytest suite by file and verify the CI reports."""

from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
import sys


SHARD_COUNTS = {
    ('native', '3.10'): 4,
    ('native', '3.11'): 3,
    ('native', '3.12'): 4,
    ('native', '3.13'): 4,
    ('python', '3.11'): 2,
}
UPDATE_KINDS = ('row_id', 'grouped', 'predicate', 'upsert', 'incremental', 'merge', 'shard')
COVERAGE_KEYS = ('plan', 'read', 'write', 'commit') + tuple('update.' + kind for kind in UPDATE_KINDS)


def matrix_entries():
    return [
        {'execution': execution, 'python-version': version, 'shard': index}
        for (execution, version), count in SHARD_COUNTS.items()
        for index in range(1, count + 1)
    ]


def file_assignments(file_counts, durations, shard_count):
    # Approximate unprofiled files by the number of tests they contribute.
    weights = {path: durations.get(path, 0.1 + 0.015 * ntests) for path, ntests in file_counts.items()}
    loads = [0.0] * shard_count
    assignments = {}
    for path in sorted(weights, key=lambda path: (-weights[path], path)):
        shard = min(range(shard_count), key=loads.__getitem__)
        assignments[path] = shard + 1
        loads[shard] += weights[path]
    return assignments


def select_shard(items, execution, version, index):
    shard_count = SHARD_COUNTS[execution, version]
    if not 1 <= index <= shard_count:
        raise ValueError('Invalid pytest shard {} of {}'.format(index, shard_count))
    duration_column = 1 if execution == 'native' else 2
    durations = {}
    for line in Path(__file__).with_name('ci_shard_durations.tsv').read_text().splitlines():
        if line and not line.startswith('#'):
            columns = line.split('\t')
            duration = float(columns[duration_column])
            if duration > 0:
                durations['pypaimon/tests/' + columns[0]] = duration

    paths = [item.nodeid.split('::', 1)[0] for item in items]
    assignments = file_assignments(Counter(paths), durations, shard_count)
    selected, deselected = [], []
    for item, path in zip(items, paths):
        (selected if assignments[path] == index else deselected).append(item)
    return selected, deselected, {
        'execution': execution,
        'python': version,
        'shard': index,
        'collection': sha256('\n'.join(sorted(item.nodeid for item in items)).encode('utf-8')).hexdigest(),
        'collected': len(items),
        'selected': len(selected),
    }


def verify_reports(reports):
    expected = {(item['execution'], item['python-version'], item['shard']) for item in matrix_entries()}
    received = [(report['execution'], report['python'], report['shard']) for report in reports]
    if len(received) != len(expected) or set(received) != expected:
        raise ValueError('Expected one report from each pytest shard, received {}'.format(sorted(received)))

    for execution, version in SHARD_COUNTS:
        group = [report for report in reports if (report['execution'], report['python']) == (execution, version)]
        label = '{} / Python {}'.format(execution, version)
        collections = {(report['collection'], report['collected']) for report in group}
        if len(collections) != 1:
            raise ValueError('{} shards collected different tests'.format(label))
        _, collected = collections.pop()
        selected = sum(report['selected'] for report in group)
        if selected != collected or any(report['selected'] <= 0 for report in group):
            raise ValueError('{} shards selected {} of {} tests'.format(label, selected, collected))
        print('{} exercised {} tests across {} shards'.format(label, selected, len(group)))

        if execution == 'native':
            counts = Counter()
            for report in group:
                counts.update(report['native'])
            missing = [key for key in COVERAGE_KEYS if counts[key] <= 0]
            if missing:
                raise ValueError('{} did not exercise {}'.format(label, ', '.join(missing)))
            print(', '.join('{}={}'.format(key, counts[key]) for key in COVERAGE_KEYS))


if __name__ == '__main__':
    if sys.argv[1] == 'matrix':
        print('matrix=' + json.dumps(matrix_entries(), separators=(',', ':')))
    else:
        verify_reports([json.loads(path.read_text()) for path in Path(sys.argv[1]).glob('*/pytest-shard.json')])
