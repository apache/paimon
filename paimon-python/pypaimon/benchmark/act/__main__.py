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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Command-line entry point for ACT benchmark preparation, runs, and reports."""

# ruff: noqa: E402

import sys


def _require_supported_python(version_info):
    """Reject runtimes older than the ACT dependencies support."""
    if tuple(version_info[:2]) < (3, 10):
        raise RuntimeError("ACT benchmark requires Python 3.10 or newer.")


_require_supported_python(sys.version_info)

import argparse
import copy
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pypaimon.benchmark.act.compare import (
    compare_results,
    load_result_documents,
)
from pypaimon.benchmark.act.experiment import load_experiment
from pypaimon.benchmark.act.runner import prepare_experiment, run_experiment


_CONFIG_ARGUMENTS = (
    ("seed", int),
    ("action_horizon", int),
    ("batch_size", int),
    ("optimizer_steps", int),
    ("image_height", int),
    ("image_width", int),
    ("learning_rate", float),
    ("weight_decay", float),
    ("warmup_batches", int),
    ("timed_batches", int),
    ("fetch_batches", int),
    ("rounds", int),
)


def main(argv=None):
    """Parse an ACT benchmark subcommand and write its JSON artifact."""
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command == "prepare":
        definition = copy.deepcopy(load_experiment(args.experiment))
        for name, _ in _CONFIG_ARGUMENTS:
            value = getattr(args, name)
            if value is not None:
                definition["config"][name] = value
        for name in (
                "statistics_version", "train_episode_id",
                "validation_episode_id"):
            value = getattr(args, name)
            if value is not None:
                definition[name] = value
        output = Path(args.output)
        experiment = prepare_experiment(
            args.input,
            args.warehouse,
            output,
            definition=definition,
            database=args.database,
        )
        _print_artifact("experiment", output, experiment["schema_version"])
        return 0
    if args.command == "run":
        experiment = load_experiment(args.experiment)
        output = (
            Path(args.output)
            if args.output else _artifact_path(
                args.results_dir,
                "%s-%s" % (experiment["benchmark_id"], args.backend),
            )
        )
        result = run_experiment(
            args.backend,
            args.experiment,
            output,
            input_root=args.input,
            warehouse=args.warehouse,
        )
        _print_artifact("result", output, result["status"])
        return 0
    results_dir = args.results_dir
    if not args.results and results_dir is None:
        results_dir = "act-results"
    results = load_result_documents(args.results, results_dir=results_dir)
    comparison = compare_results(results)
    output = (
        Path(args.output)
        if args.output else _artifact_path(
            results_dir or "act-results", "comparison")
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(comparison, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _print_artifact("comparison", output, comparison["status"])
    return 0 if comparison["status"] == "SUCCEEDED" else 1


def _parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser(
        "prepare",
        help="Resolve a shared experiment against matching HDF5 and Paimon data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    prepare.add_argument("--input", required=True, help="RoboMIND HDF5 root.")
    prepare.add_argument("--warehouse", required=True, help="Paimon warehouse.")
    prepare.add_argument(
        "--experiment",
        help="Input experiment JSON; packaged defaults are used when omitted.",
    )
    prepare.add_argument(
        "--output", default="act-results/experiment.json",
        help="Resolved experiment JSON path.")
    prepare.add_argument("--database", default="robomind")
    prepare.add_argument("--statistics-version")
    prepare.add_argument("--train-episode-id")
    prepare.add_argument("--validation-episode-id")
    for name, argument_type in _CONFIG_ARGUMENTS:
        prepare.add_argument(
            "--" + name.replace("_", "-"), type=argument_type, default=None)

    run = commands.add_parser(
        "run",
        help="Run one storage backend using a resolved experiment.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    run.add_argument("--backend", required=True, choices=("hdf5", "paimon"))
    run.add_argument("--experiment", required=True)
    run.add_argument("--input", help="HDF5 root; required for backend=hdf5.")
    run.add_argument(
        "--warehouse", help="Paimon warehouse; required for backend=paimon.")
    run.add_argument("--output", help="Explicit result JSON path.")
    run.add_argument(
        "--results-dir", default="act-results",
        help="Directory for an automatically named result.")

    compare = commands.add_parser(
        "compare",
        help=(
            "Group results by experiment and aggregate compatible repeats."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    compare.add_argument("results", nargs="*", help="Explicit result JSON files.")
    compare.add_argument(
        "--results-dir",
        help="Also discover ACT result JSON files in this directory.")
    compare.add_argument("--output", help="Comparison JSON path.")
    return parser


def _artifact_path(directory, prefix):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path(directory).expanduser() / (
        "%s-%s-%s.json" % (prefix, timestamp, uuid.uuid4().hex[:8]))


def _print_artifact(kind, path, status):
    print(json.dumps({
        "artifact": str(Path(path).expanduser().resolve()),
        "kind": kind,
        "status": status,
    }, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
