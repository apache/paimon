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

"""The required CI check must not pass when a selected group did not pass."""

import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import unittest


class ResultTest(unittest.TestCase):
    def run_gate(self, plan_result='success', **outcomes):
        workflow = Path(__file__).resolve().parents[2] / '.github/workflows/ci.yml'
        source = workflow.read_text().split("python3 - <<'PYTHON'\n", 1)[1]
        source = textwrap.dedent(source.split('          PYTHON', 1)[0])
        flags = dict(java='true', python='false', docs='false', licensing='true')
        results = dict(plan=dict(result=plan_result, outputs=flags),
                       java=dict(result='success'), python=dict(result='skipped'),
                       docs=dict(result='skipped'), licensing=dict(result='success'))
        for name, result in outcomes.items():
            results[name]['result'] = result
        return subprocess.run([sys.executable, '-c', source], capture_output=True,
                              env=dict(os.environ, RESULTS=json.dumps(results)))

    def test_selected_groups_pass_and_unselected_groups_skip(self):
        self.assertEqual(self.run_gate().returncode, 0)

    def test_selected_failure_cancellation_and_unexpected_skip_fail(self):
        for outcome in ('failure', 'cancelled', 'skipped'):
            with self.subTest(outcome=outcome):
                self.assertNotEqual(self.run_gate(java=outcome).returncode, 0)

    def test_planner_failure_fails_even_if_other_groups_report_success(self):
        for outcome in ('failure', 'cancelled', 'skipped'):
            self.assertNotEqual(self.run_gate(plan_result=outcome).returncode, 0)


if __name__ == '__main__':
    unittest.main()
