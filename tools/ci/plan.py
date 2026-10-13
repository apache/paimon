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

"""Select CI suites from the complete git diff, without API pagination limits."""

import json
import os
from pathlib import Path
import re
import subprocess


# Keep the existing JDK, engine and Scala coverage. A suite is a test boundary,
# not just a source directory: core also owns Hive and generated-doc tests.
SUITES = [
    ("core", "Core and integrations / JDK 8", 8, "2.12", 90),
    ("core", "Core and integrations / JDK 11", 11, "2.12", 90),
    ("flink1-common", "Flink 1 / Common", 8, "2.12", 100),
    ("flink1-connectors", "Flink 1 / Connectors and CDC", 8, "2.12", 60),
    ("flink2", "Flink 2 / Common and connectors", 11, "2.12", 120),
    ("spark3", "Spark 3 / Scala 2.12", 8, "2.12", 90),
    ("spark3", "Spark 3 / Scala 2.13", 8, "2.13", 90),
    ("spark4", "Spark 4 / Scala 2.13", 17, "2.13", 90),
    ("e2e-flink1", "E2E / Flink 1 and Spark interoperability", 8, "2.12", 60),
    ("e2e-flink2", "E2E / Flink 2", 11, "2.12", 60),
    ("iceberg", "Iceberg GA / JDK 17", 17, "2.12", 60),
    ("eslib", "ESLib / JDK 11", 11, "2.12", 30),
    ("full-text", "Full text / JDK 8", 8, "2.12", 30),
]
ALL_JAVA = {suite[0] for suite in SUITES}
FLINK = {"flink1-common", "flink1-connectors", "flink2"}
SPARK = {"spark3", "spark4"}
E2E = {"e2e-flink1", "e2e-flink2"}


def select(paths, full=False):
    selected = set()
    docs = python = licensing = False
    if full:
        selected.update(ALL_JAVA)
        docs = python = licensing = True

    for path in paths:
        if path.startswith("docs/"):
            docs = True
            continue
        if path.endswith(".md"):
            continue
        if path.startswith("paimon-python/") or path == ".github/workflows/ci-python.yml":
            python = True
            continue
        if re.search(r"/src/test/.*?/JavaPy[^/]*\.java$", path):
            python = True
            continue
        if path == ".github/workflows/ci-docs.yml":
            docs = True
            continue
        if path == ".github/workflows/ci-licensing.yml":
            licensing = True
            continue
        # Changes to CI machinery must validate every consumer of that machinery.
        if path.startswith((".github/workflows/ci", "tools/ci/")):
            selected.update(ALL_JAVA)
            docs = python = licensing = True
            continue
        if path.startswith((".github/", ".asf.yaml")):
            continue

        licensing = True
        if path.startswith("paimon-spark/"):
            # Core includes spark-common tests and paimon-docs completeness tests.
            # SparkE2eTest is disabled on JDK 11, so only the Flink 1 E2E lane applies.
            selected.update(SPARK | {"core", "e2e-flink1"})
        elif path.startswith("paimon-flink/"):
            # Hive and Iceberg integration tests depend on flink-common.
            selected.update(FLINK | E2E | {"core", "iceberg"})
        elif path.startswith("paimon-e2e-tests/"):
            selected.update(E2E)
        elif path.startswith("paimon-eslib/"):
            selected.add("eslib")
        elif path.startswith("paimon-iceberg/"):
            selected.update({"core", "iceberg"})
        elif path.startswith("paimon-full-text/"):
            selected.update({"core", "full-text"})
        else:
            # Shared modules, POMs outside a known leaf, build configuration and
            # new/unknown directories fall back to all Java suites.
            selected.update(ALL_JAVA)
            if path == "pom.xml":
                python = True

    matrix = [
        dict(suite=suite, name=name, java=java, scala=scala, timeout=timeout)
        for suite, name, java, scala, timeout in SUITES
        if suite in selected
    ]
    return dict(java=bool(matrix), matrix={"include": matrix}, docs=docs,
                python=python, licensing=licensing)


def changed_paths(event_name, event):
    if event_name == "pull_request":
        base = event["pull_request"]["base"]["sha"]
        head = event["pull_request"]["head"]["sha"]
        separator = "..."
    elif event_name == "push":
        base, head = event["before"], event["after"]
        separator = ".."
    else:
        return None
    for sha in (base, head):
        if not re.fullmatch(r"[0-9a-f]{40}", sha) or sha == "0" * 40:
            return None
    try:
        # Disabling rename detection includes both the old and new path. -z also
        # handles whitespace, newlines and non-ASCII names without shell parsing.
        output = subprocess.check_output([
            "git", "diff", "--no-renames", "--name-only", "-z",
            base + separator + head, "--",
        ])
    except subprocess.CalledProcessError:
        print("Cannot resolve the complete diff; selecting full CI.")
        return None
    return [os.fsdecode(path) for path in output.split(b"\0") if path]


def check_file_sizes(paths, root=Path(".")):
    for path in paths:
        candidate = root / path
        # Git symlinks contain a link target; don't follow them outside checkout.
        if candidate.is_symlink() or not candidate.is_file():
            continue
        if candidate.stat().st_size > 1048576:
            raise ValueError("Changed file exceeds 1 MiB: " + repr(path))


def main():
    event_name = os.environ["GITHUB_EVENT_NAME"]
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    paths = changed_paths(event_name, event)
    plan = select(paths or [], full=paths is None)
    size_warning = None
    if event_name == "pull_request":
        if paths is None:
            # Existing oversized files must not block the full-CI fallback.
            size_warning = "Skipping changed-file size check because the PR diff is unavailable."
            print("::warning::" + size_warning)
        else:
            check_file_sizes(paths)
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        for key, value in plan.items():
            output.write(key + "=" + json.dumps(value, separators=(",", ":")) + "\n")
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write("## CI selection\n\n")
        summary.write("Full run (manual or unavailable diff).\n\n" if paths is None
                      else "Compared {} changed paths.\n\n".format(len(paths)))
        if size_warning:
            summary.write("**Warning:** " + size_warning + "\n\n")
        for lane in plan["matrix"]["include"]:
            summary.write("- " + lane["name"] + "\n")
        for key in ("python", "docs", "licensing"):
            summary.write("- {}: {}\n".format(key, "run" if plan[key] else "skip"))
    print(json.dumps(plan, indent=2))


if __name__ == "__main__":
    main()
