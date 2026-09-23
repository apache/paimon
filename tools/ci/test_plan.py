#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Behavioral checks for CI routing and the complete PR/push diff."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import plan


class SelectionTest(unittest.TestCase):
    def suites(self, paths):
        return {lane['suite'] for lane in plan.select(paths)['matrix']['include']}

    def test_spark_keeps_common_docs_and_interoperability_tests(self):
        suites = self.suites(['paimon-spark/paimon-spark-common/src/main/scala/Change.scala'])
        self.assertEqual(suites, {'core', 'spark3', 'spark4', 'e2e-flink1'})

    def test_flink_keeps_hive_iceberg_and_both_e2e_versions(self):
        suites = self.suites(['paimon-flink/paimon-flink-common/src/main/java/Change.java'])
        self.assertEqual(suites, {'core', 'flink1-common', 'flink1-connectors',
                                  'flink2', 'e2e-flink1', 'e2e-flink2', 'iceberg'})

    def test_shared_and_unknown_changes_run_all_java(self):
        for path in ['paimon-core/Change.java', 'paimon-api/Change.java',
                     'paimon-format/pom.xml', 'paimon-new-module/pom.xml',
                     '.mvn/jvm.config', '.scalafmt.conf', 'copyright.txt']:
            with self.subTest(path=path):
                self.assertEqual(self.suites([path]), plan.ALL_JAVA)

    def test_parent_pom_also_validates_python_build(self):
        self.assertTrue(plan.select(['pom.xml'])['python'])
        self.assertEqual(self.suites(['pom.xml']), plan.ALL_JAVA)

    def test_docs_and_markdown_do_not_start_maven(self):
        result = plan.select(['docs/start.md', 'README.md'])
        self.assertTrue(result['docs'])
        self.assertFalse(result['java'])
        self.assertFalse(result['python'])
        self.assertFalse(result['licensing'])
        self.assertFalse(plan.select(['paimon-python/README.md'])['python'])

    def test_mixed_changes_take_union(self):
        self.assertEqual(self.suites(['docs/intro.md', 'paimon-spark/pom.xml',
                                      'paimon-flink/pom.xml']),
                         {'core', 'spark3', 'spark4', 'flink1-common',
                          'flink1-connectors', 'flink2', 'e2e-flink1',
                          'e2e-flink2', 'iceberg'})
        result = plan.select(['paimon-python/setup.py', 'paimon-core/Change.java'])
        self.assertTrue(result['python'])
        self.assertTrue(result['java'])

    def test_java_python_fixtures_run_python(self):
        for module, name in [('paimon-core', 'JavaPyE2ETest'),
                             ('paimon-lance', 'JavaPyLanceE2ETest'),
                             ('paimon-lumina', 'JavaPyLuminaE2ETest')]:
            result = plan.select([module + '/src/test/java/org/apache/paimon/' + name + '.java'])
            self.assertTrue(result['python'])
            self.assertFalse(result['java'])

    def test_leaf_modules_and_workflows(self):
        self.assertEqual(self.suites(['paimon-eslib/pom.xml']), {'eslib'})
        self.assertEqual(self.suites(['paimon-e2e-tests/pom.xml']),
                         {'e2e-flink1', 'e2e-flink2'})
        self.assertTrue(plan.select(['.github/workflows/ci-python.yml'])['python'])
        self.assertFalse(plan.select(['.github/workflows/ci-python.yml'])['java'])
        self.assertTrue(plan.select(['.github/workflows/ci-docs.yml'])['docs'])
        self.assertFalse(plan.select(['.github/workflows/release-java.yml'])['java'])

    def test_ci_logic_and_manual_run_validate_every_consumer(self):
        for result in [plan.select(['tools/ci/plan.py']),
                       plan.select(['.github/workflows/ci-java.yml']),
                       plan.select([], full=True)]:
            self.assertTrue(all(result[key] for key in ('java', 'python', 'docs', 'licensing')))
            self.assertEqual(len(result['matrix']['include']), 13)

    def test_empty_diff_does_not_run_expensive_jobs(self):
        result = plan.select([])
        self.assertFalse(any(result[key] for key in ('java', 'python', 'docs', 'licensing')))
        self.assertEqual(result['matrix'], {'include': []})

    def test_size_guard_handles_spaces_deletions_and_symlinks(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            large = root / 'large file.txt'
            large.write_bytes(b'x' * (1048576 + 1))
            (root / 'link').symlink_to(large)
            plan.check_file_sizes(['deleted.txt', 'link'], root)
            with self.assertRaisesRegex(ValueError, 'large file'):
                plan.check_file_sizes(['large file.txt'], root)


class GitDiffTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.previous = Path.cwd()
        os.chdir(self.temp.name)
        self.addCleanup(os.chdir, self.previous)
        self.git('init', '-q')
        self.git('config', 'user.email', 'ci-test@example.invalid')
        self.git('config', 'user.name', 'CI test')
        self.commit('base.txt', 'base')
        self.base = self.git('rev-parse', 'HEAD')

    def git(self, *args):
        return subprocess.check_output(['git', *args], stderr=subprocess.DEVNULL).decode().strip()

    def commit(self, name, content):
        path = Path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        self.git('add', '--', name)
        self.git('commit', '-qm', 'Test commit')

    def test_pr_uses_merge_base_instead_of_including_base_branch_changes(self):
        self.commit('base-only.txt', 'base change')
        base_tip = self.git('rev-parse', 'HEAD')
        self.git('checkout', '-q', '--detach', self.base)
        self.commit('paimon-spark/name with spaces\nand newline.scala', 'spark')
        head = self.git('rev-parse', 'HEAD')
        paths = plan.changed_paths('pull_request', {
            'pull_request': {'base': {'sha': base_tip}, 'head': {'sha': head}}})
        self.assertEqual(paths, ['paimon-spark/name with spaces\nand newline.scala'])

    def test_push_includes_all_commits_and_both_sides_of_renames(self):
        self.commit('paimon-spark/A.scala', 'spark')
        before = self.git('rev-parse', 'HEAD')
        Path('paimon-flink').mkdir()
        self.git('mv', 'paimon-spark/A.scala', 'paimon-flink/A.java')
        self.git('commit', '-qm', 'Move across modules')
        self.commit('docs/intro.md', 'docs')
        paths = plan.changed_paths('push', {'before': before,
                                          'after': self.git('rev-parse', 'HEAD')})
        self.assertEqual(set(paths), {'paimon-spark/A.scala', 'paimon-flink/A.java',
                                     'docs/intro.md'})

    def test_large_diffs_do_not_truncate_late_core_changes(self):
        for index in range(350):
            Path('doc{:03d}.md'.format(index)).write_text('docs')
        self.commit('z-new-core/Change.java', 'core')
        self.git('add', '.')
        self.git('commit', '-qm', 'Many docs')
        paths = plan.changed_paths('push', {'before': self.base,
                                          'after': self.git('rev-parse', 'HEAD')})
        self.assertEqual(len(paths), 351)
        self.assertTrue(plan.select(paths)['java'])

    def test_new_branch_missing_commits_and_manual_run_fall_back(self):
        self.assertIsNone(plan.changed_paths('push', {'before': '0' * 40, 'after': self.base}))
        self.assertIsNone(plan.changed_paths('workflow_dispatch', {}))
        with patch('subprocess.check_output', side_effect=subprocess.CalledProcessError(128, 'git')):
            self.assertIsNone(plan.changed_paths('push', {'before': 'a' * 40, 'after': self.base}))

    def run_pr_plan(self, base, head):
        Path('event.json').write_text(json.dumps({
            'pull_request': {'base': {'sha': base}, 'head': {'sha': head}}}))
        env = {'GITHUB_EVENT_NAME': 'pull_request', 'GITHUB_EVENT_PATH': 'event.json',
               'GITHUB_OUTPUT': 'output.txt', 'GITHUB_STEP_SUMMARY': 'summary.md'}
        with patch.dict(os.environ, env), contextlib.redirect_stdout(io.StringIO()) as log:
            plan.main()
        outputs = dict(line.split('=', 1) for line in Path('output.txt').read_text().splitlines())
        return {key: json.loads(value) for key, value in outputs.items()}, log.getvalue()

    def test_pr_unavailable_diff_runs_full_ci_with_unchanged_oversized_file(self):
        self.commit('docs/static/old image.png', 'x' * (1048576 + 1))
        base = self.git('rev-parse', 'HEAD')
        self.commit('paimon-spark/Change.scala', 'spark')
        head = self.git('rev-parse', 'HEAD')
        git_output = subprocess.check_output

        def unavailable_diff(command, *args, **kwargs):
            if command[:2] == ['git', 'diff']:
                raise subprocess.CalledProcessError(128, command)
            return git_output(command, *args, **kwargs)

        with patch('subprocess.check_output', side_effect=unavailable_diff):
            outputs, log = self.run_pr_plan(base, head)
        self.assertTrue(all(outputs[key] for key in ('java', 'python', 'docs', 'licensing')))
        self.assertEqual(len(outputs['matrix']['include']), 13)
        summary = Path('summary.md').read_text()
        self.assertIn('Full run (manual or unavailable diff).', summary)
        self.assertIn('Skipping changed-file size check', log)
        self.assertIn('Skipping changed-file size check', summary)

    def test_pr_size_guard_ignores_unchanged_but_rejects_changed_oversized_files(self):
        self.commit('docs/static/old image.png', 'x' * (1048576 + 1))
        base = self.git('rev-parse', 'HEAD')
        self.commit('paimon-spark/Change.scala', 'spark')
        outputs, _ = self.run_pr_plan(base, self.git('rev-parse', 'HEAD'))
        self.assertEqual(len(outputs['matrix']['include']), 6)

        for path in ('docs/static/old image.png', 'docs/static/new image.png'):
            with self.subTest(path=path):
                base = self.git('rev-parse', 'HEAD')
                self.commit(path, 'y' * (1048576 + 1))
                with self.assertRaisesRegex(ValueError, 'Changed file exceeds 1 MiB') as error:
                    self.run_pr_plan(base, self.git('rev-parse', 'HEAD'))
                self.assertIn(path, str(error.exception))

    def test_github_outputs_are_valid_json_and_summary_explains_selection(self):
        self.commit('paimon-spark/Change.scala', 'spark')
        Path('event.json').write_text(json.dumps({'before': self.base,
                                               'after': self.git('rev-parse', 'HEAD')}))
        env = {'GITHUB_EVENT_NAME': 'push', 'GITHUB_EVENT_PATH': 'event.json',
               'GITHUB_OUTPUT': 'output.txt', 'GITHUB_STEP_SUMMARY': 'summary.md'}
        with patch.dict(os.environ, env), contextlib.redirect_stdout(None):
            plan.main()
        outputs = dict(line.split('=', 1) for line in Path('output.txt').read_text().splitlines())
        self.assertTrue(json.loads(outputs['java']))
        self.assertFalse(json.loads(outputs['python']))
        self.assertEqual(len(json.loads(outputs['matrix'])['include']), 6)
        self.assertIn('Spark interoperability', Path('summary.md').read_text())


if __name__ == '__main__':
    unittest.main()
