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

"""Validate suite coverage and Maven command boundaries without running Maven."""

import os
from pathlib import Path
import re
import shlex
import subprocess
import unittest
import xml.etree.ElementTree as ET

from plan import SUITES


SCRIPT = Path(__file__).with_name('run-java-tests.sh')


class MavenCommandsTest(unittest.TestCase):
    def commands(self, suite, scala='2.12', java=8):
        output = subprocess.check_output(
            ['bash', str(SCRIPT), suite, scala, str(java)],
            env=dict(os.environ, CI_DRY_RUN='true'), text=True)
        return [shlex.split(line) for line in output.splitlines()]

    def modules(self, command):
        return set(command[command.index('-pl') + 1].split(','))

    def test_every_lane_builds_dependencies_but_only_tests_selected_modules(self):
        for suite, name, java, scala, timeout in SUITES:
            with self.subTest(name=name):
                build, test = self.commands(suite, scala, java)
                self.assertIn('-am', build)
                self.assertIn('-DskipTests', build)
                self.assertNotIn('-am', test)
                self.assertNotIn('-DskipTests', test)
                self.assertEqual(self.modules(build), self.modules(test))
                self.assertNotIn('clean', test)
                self.assertFalse('test' in test and 'verify' in test)
                self.assertFalse(any('fast-build' in arg for arg in build + test))

    def test_spark_retains_all_connector_versions_and_scala_variants(self):
        for scala in ('2.12', '2.13'):
            test = self.commands('spark3', scala)[1]
            for version in ('ut', '3.2', '3.3', '3.4', '3.5'):
                self.assertIn('org.apache.paimon:paimon-spark-' + version + '_' + scala,
                              self.modules(test))
            self.assertIn('-Pflink1,spark3,scala-' + scala, test)
        self.assertEqual(self.modules(self.commands('spark4', '2.13', 17)[1]),
                         {'org.apache.paimon:paimon-spark-' + version + '_2.13'
                          for version in ('ut', '4.0', '4.1')})

    def test_core_keeps_common_and_docs_tests_and_existing_jdk11_exclusion(self):
        for java in (8, 11):
            test = self.commands('core', java=java)[1]
            self.assertIn('-Pskip-paimon-flink-tests', test)
            self.assertNotIn('!org.apache.paimon:paimon-spark-common_2.12', self.modules(test))
            self.assertNotIn('!paimon-docs', self.modules(test))
            self.assertEqual('!org.apache.paimon:paimon-hive-connector-3.1' in self.modules(test),
                             java == 11)

    def test_flink_coverage_includes_cdc_and_both_major_versions(self):
        self.assertEqual(self.modules(self.commands('flink1-connectors')[1]),
                         {'org.apache.paimon:paimon-flink-' + version
                          for version in ('cdc', '1.16', '1.17', '1.18', '1.19', '1.20')})
        self.assertEqual(self.modules(self.commands('flink2', java=11)[1]),
                         {'org.apache.paimon:paimon-flink-' + version
                          for version in ('common', '2.0', '2.1', '2.2')})

    def test_e2e_uses_the_same_version_when_building_and_testing(self):
        for suite, java, version in [('e2e-flink1', 8, '1.20'), ('e2e-flink2', 11, '2.2')]:
            build, test = self.commands(suite, java=java)
            self.assertTrue(any('flink-' + version in arg for arg in build))
            self.assertEqual([arg for arg in build if arg.startswith('-P')],
                             [arg for arg in test if arg.startswith('-P')])
            self.assertIn('test', test)

    def test_unknown_suite_fails_instead_of_silently_running_no_tests(self):
        result = subprocess.run(['bash', str(SCRIPT), 'typo'],
                                env=dict(os.environ, CI_DRY_RUN='true'), capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'Unknown CI suite', result.stderr)

    def test_python_build_selects_runnable_modules_not_just_aggregator_poms(self):
        root = SCRIPT.resolve().parents[2]
        workflow = (root / '.github/workflows/ci-python.yml').read_text()
        modules = set(re.search(r'-pl ([^\s]+) -am', workflow).group(1).split(','))
        self.assertTrue({'paimon-core', 'paimon-lance', 'paimon-lumina',
                         'paimon-vortex/paimon-vortex-format', 'paimon-full-text',
                         'paimon-vector'}.issubset(modules))
        for module in modules:
            pom = ET.parse(root / module / 'pom.xml').getroot()
            packaging = pom.findtext('{http://maven.apache.org/POM/4.0.0}packaging', 'jar')
            self.assertNotEqual(packaging, 'pom', module)


if __name__ == '__main__':
    unittest.main()
