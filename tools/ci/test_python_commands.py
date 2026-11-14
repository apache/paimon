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

"""Check Python CI command boundaries without running Java interoperability tests."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[2] / 'paimon-python' / 'dev' / 'lint-python.sh'
NATIVE_STAGES = ('PLAN', 'READ', 'WRITE', 'UPDATE', 'COMMIT')


class PythonCommandsTest(unittest.TestCase):
    def test_mixed_sessions_keep_execution_without_full_suite_coverage_gates(self):
        for execution in ('native', 'python'):
            for mixed_status in (0, 17):
                with self.subTest(execution=execution, status=mixed_status), tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    dev = root / 'dev'
                    dev.mkdir()
                    script = dev / SCRIPT.name
                    shutil.copyfile(str(SCRIPT), str(script))
                    capture = root / 'environment.json'
                    runner = dev / 'run_mixed_tests.sh'
                    runner.write_text(
                        '#!/usr/bin/env bash\n'
                        '"$CI_TEST_PYTHON" -c \'import json, os; '
                        'print(json.dumps({k: v for k, v in os.environ.items() '
                        'if k.startswith("PYPAIMON_TEST_")}))\' > "$CI_TEST_CAPTURE"\n'
                        'exit "$CI_TEST_MIXED_STATUS"\n')
                    env = dict(
                        os.environ,
                        CI_TEST_PYTHON=sys.executable,
                        CI_TEST_CAPTURE=str(capture),
                        CI_TEST_MIXED_STATUS=str(mixed_status),
                        PYPAIMON_TEST_EXECUTION=execution,
                    )
                    env.update({'PYPAIMON_TEST_NATIVE_' + stage: '1' for stage in NATIVE_STAGES})
                    result = subprocess.run(
                        ['bash', str(script), '-i', 'mixed'], env=env,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
                    self.assertEqual(0 if mixed_status == 0 else 1, result.returncode, result.stdout)
                    child_env = json.loads(capture.read_text())
                    self.assertEqual(execution, child_env['PYPAIMON_TEST_EXECUTION'])
                    for stage in NATIVE_STAGES:
                        self.assertEqual('0', child_env.get('PYPAIMON_TEST_NATIVE_' + stage), stage)


if __name__ == '__main__':
    unittest.main()
