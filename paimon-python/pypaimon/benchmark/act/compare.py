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

"""Validate and aggregate independently produced ACT benchmark results."""

import hashlib
import json
from pathlib import Path


_METRICS = {
    "batch_fetch_samples_per_s": "higher",
    "dataset_build_s": "lower",
    "first_batch_s": "lower",
    "fixed_steps_s": "lower",
    "python_peak_allocated_bytes": "lower",
    "wall_time_s": "lower",
}


def canonical_sha256(value):
    """Return the SHA-256 of a JSON value using canonical serialization."""
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_result_documents(paths, results_dir=None):
    """Load explicit result files plus ACT results discovered in a directory.

    Explicit paths must contain result documents. Directory discovery ignores
    experiment and prior comparison JSON files. A path found both ways is read
    once, preserving explicit-path order followed by sorted directory entries.
    """
    candidates = [Path(path).expanduser().resolve() for path in paths]
    explicit = set(candidates)
    if results_dir is not None:
        directory = Path(results_dir).expanduser().resolve()
        candidates.extend(sorted(directory.glob("*.json")))
    seen = set()
    results = []
    for path in candidates:
        path = path.resolve()
        if path in seen:
            continue
        seen.add(path)
        with path.open(encoding="utf-8") as result_file:
            document = json.load(result_file)
        if document.get("schema_version") != "act-benchmark-result@1":
            if path in explicit:
                raise ValueError("Not an ACT benchmark result: %s." % path)
            continue
        results.append(document)
    if not results:
        raise ValueError("No ACT benchmark result files were found.")
    return results


def compare_results(results):
    """Group result documents by experiment and compare compatible backends.

    Results from different experiment definitions remain separate. Results in
    one experiment group must report the same runtime environment; otherwise
    the group is marked incompatible and no performance ratios are produced.

    Args:
        results: Iterable of decoded ``act-benchmark-result@1`` documents.

    Returns:
        A JSON-compatible comparison document with one entry per experiment.
    """
    groups = {}
    for result in results:
        if result.get("schema_version") != "act-benchmark-result@1":
            raise ValueError("Unsupported ACT benchmark result schema.")
        experiment = result.get("experiment")
        if not isinstance(experiment, dict):
            raise ValueError("ACT benchmark result has no experiment object.")
        experiment_sha256 = canonical_sha256(experiment)
        if result.get("experiment_sha256") != experiment_sha256:
            raise ValueError("ACT result experiment SHA-256 differs.")
        if result.get("status") != "SUCCEEDED":
            raise ValueError("ACT comparison requires successful results.")
        if result.get("backend") not in ("hdf5", "paimon"):
            raise ValueError("ACT result has an unsupported backend.")
        groups.setdefault(experiment_sha256, []).append(result)

    experiments = [
        _compare_experiment(experiment_sha256, grouped)
        for experiment_sha256, grouped in sorted(groups.items())
    ]
    statuses = {item["status"] for item in experiments}
    if statuses == {"SUCCEEDED"}:
        status = "SUCCEEDED"
    elif "FAILED" in statuses:
        status = "FAILED"
    else:
        status = "INCOMPATIBLE"
    return {
        "schema_version": "act-benchmark-comparison@1",
        "status": status,
        "experiments": experiments,
    }


def _compare_experiment(experiment_sha256, results):
    environments = {
        canonical_sha256(result.get("environment", {})) for result in results
    }
    by_backend = {}
    for result in results:
        by_backend.setdefault(result["backend"], []).append(result)
    if set(by_backend) != {"hdf5", "paimon"}:
        return {
            "experiment_sha256": experiment_sha256,
            "experiment": results[0]["experiment"],
            "status": "INCOMPATIBLE",
            "reason": "both hdf5 and paimon results are required",
            "backends": sorted(by_backend),
            "result_count": len(results),
            "metrics": {},
        }
    if len(environments) != 1:
        return {
            "experiment_sha256": experiment_sha256,
            "experiment": results[0]["experiment"],
            "status": "INCOMPATIBLE",
            "reason": "runtime environments differ",
            "environment_sha256s": sorted(environments),
            "backends": sorted(by_backend),
            "metrics": {},
        }
    models = {canonical_sha256(result.get("model")) for result in results}
    if len(models) != 1:
        return _failed_group(
            experiment_sha256, by_backend, results, "models differ")
    fingerprints = {
        result.get("tensor_fingerprint", {}).get("sha256")
        for result in results
    }
    if len(fingerprints) != 1 or None in fingerprints:
        return _failed_group(
            experiment_sha256,
            by_backend,
            results,
            "tensor fingerprints differ",
        )
    loss_traces = {canonical_sha256([{
        "round": run["round"],
        "train_loss": run["train_loss"],
        "validation_loss": run["validation_loss"],
    } for run in result.get("runs", [])]) for result in results}
    if len(loss_traces) != 1:
        return _failed_group(
            experiment_sha256, by_backend, results, "loss traces differ")

    medians = {
        backend: _aggregate_backend(items)
        for backend, items in by_backend.items()
    }
    metrics = {}
    for name, preferred in _METRICS.items():
        values = {
            backend: summary[name]
            for backend, summary in medians.items()
            if name in summary
        }
        if values:
            metric = dict(values)
            metric["preferred"] = preferred
            if set(values) == {"hdf5", "paimon"}:
                if preferred == "higher" and values["hdf5"]:
                    metric["paimon_over_hdf5"] = (
                        values["paimon"] / values["hdf5"])
                elif preferred == "lower" and values["paimon"]:
                    metric["hdf5_over_paimon"] = (
                        values["hdf5"] / values["paimon"])
            metrics[name] = metric
    return {
        "experiment_sha256": experiment_sha256,
        "experiment": results[0]["experiment"],
        "status": "SUCCEEDED",
        "environment": results[0]["environment"],
        "environment_sha256": next(iter(environments)),
        "backends": sorted(by_backend),
        "result_count": len(results),
        "metrics": metrics,
    }


def _failed_group(experiment_sha256, by_backend, results, reason):
    return {
        "experiment_sha256": experiment_sha256,
        "experiment": results[0]["experiment"],
        "status": "FAILED",
        "reason": reason,
        "backends": sorted(by_backend),
        "result_count": len(results),
        "metrics": {},
    }


def _aggregate_backend(results):
    names = set.intersection(*(
        set(result.get("summary", {})) for result in results
    ))
    aggregated = {}
    for name in names:
        if name not in _METRICS:
            continue
        values = [result["summary"][name]["median"] for result in results]
        values.sort()
        middle = len(values) // 2
        aggregated[name] = (
            values[middle]
            if len(values) % 2
            else (values[middle - 1] + values[middle]) / 2.0
        )
    return aggregated
