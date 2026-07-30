################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import os
import subprocess
import sys
import textwrap
import unittest


class OptionalDataSketchesDependencyTest(unittest.TestCase):

    def test_missing_datasketches_does_not_block_regular_aggregators(self):
        code = textwrap.dedent(
            """
            import builtins

            real_import = builtins.__import__

            def import_without_datasketches(
                    name, globals=None, locals=None, fromlist=(), level=0):
                if name == "_datasketches":
                    raise ModuleNotFoundError(
                        "No module named '_datasketches'")
                return real_import(
                    name, globals, locals, fromlist, level)

            builtins.__import__ = import_without_datasketches

            import pypaimon
            from pypaimon.common.options import CoreOptions, Options
            from pypaimon.read.reader.aggregate import create_field_aggregator
            from pypaimon.schema.data_types import AtomicType

            options = CoreOptions(Options.from_none())
            sum_agg = create_field_aggregator(
                AtomicType("INT"), "value", "sum", options)
            assert sum_agg.agg(1, 2) == 3

            theta_agg = create_field_aggregator(
                AtomicType("VARBINARY"), "value", "theta_sketch", options)
            try:
                theta_agg.agg(b"first", b"second")
            except ImportError as exc:
                assert "pypaimon[theta-sketch]" in str(exc)
            else:
                raise AssertionError(
                    "theta_sketch should require datasketches")
            """
        )
        env = os.environ.copy()
        python_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", ".."))
        existing = env.get("PYTHONPATH")
        env["PYTHONPATH"] = (
            python_root
            if not existing
            else python_root + os.pathsep + existing
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        self.assertEqual(
            0,
            result.returncode,
            "subprocess failed:\nstdout:\n{}\nstderr:\n{}".format(
                result.stdout, result.stderr),
        )


if __name__ == "__main__":
    unittest.main()
