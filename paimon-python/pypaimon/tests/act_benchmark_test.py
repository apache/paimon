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

import json

import pytest

from pypaimon.benchmark.act.compare import (
    canonical_sha256,
    compare_results,
    load_result_documents,
)
from pypaimon.benchmark.act.experiment import load_experiment


def test_packaged_experiment_contains_a_valid_benchmark_config():
    _require_act_runtime()
    from pypaimon.benchmark.act.harness import BenchmarkConfig

    experiment = load_experiment()

    assert experiment["schema_version"] == "act-benchmark-experiment@1"
    assert experiment["benchmark_id"] == "robomind-act"
    assert BenchmarkConfig(**experiment["config"]).to_dict() == (
        experiment["config"])
    assert experiment["statistics_version"] == (
        "robomind-agilex-joint-position@1")


def test_compare_reports_ratio_for_matching_backend_results():
    experiment = {"schema_version": "act-benchmark-experiment@1"}
    environment = {"python": "3.10", "machine": "arm64"}
    hdf5 = _result("hdf5", experiment, environment, throughput=10.0)
    paimon = _result("paimon", experiment, environment, throughput=15.0)
    hdf5["summary"]["first_batch_s"] = {
        "median": 2.0, "min": 2.0, "max": 2.0}
    paimon["summary"]["first_batch_s"] = {
        "median": 1.0, "min": 1.0, "max": 1.0}

    comparison = compare_results([hdf5, paimon])

    assert comparison["status"] == "SUCCEEDED"
    assert len(comparison["experiments"]) == 1
    group = comparison["experiments"][0]
    assert group["backends"] == ["hdf5", "paimon"]
    assert group["metrics"]["batch_fetch_samples_per_s"] == {
        "hdf5": 10.0,
        "paimon": 15.0,
        "paimon_over_hdf5": 1.5,
        "preferred": "higher",
    }
    assert group["metrics"]["first_batch_s"] == {
        "hdf5": 2.0,
        "paimon": 1.0,
        "hdf5_over_paimon": 2.0,
        "preferred": "lower",
    }


def test_compare_rejects_different_tensor_fingerprints():
    experiment = {"schema_version": "act-benchmark-experiment@1"}
    environment = {"python": "3.10", "machine": "arm64"}
    hdf5 = _result("hdf5", experiment, environment, throughput=10.0)
    paimon = _result("paimon", experiment, environment, throughput=15.0)
    paimon["tensor_fingerprint"]["sha256"] = "different"

    comparison = compare_results([hdf5, paimon])

    assert comparison["status"] == "FAILED"
    group = comparison["experiments"][0]
    assert group["status"] == "FAILED"
    assert group["reason"] == "tensor fingerprints differ"
    assert group["metrics"] == {}


def test_compare_requires_results_from_both_backends():
    experiment = {"schema_version": "act-benchmark-experiment@1"}
    environment = {"python": "3.10", "machine": "arm64"}

    comparison = compare_results([
        _result("hdf5", experiment, environment, throughput=10.0),
    ])

    assert comparison["status"] == "INCOMPATIBLE"
    group = comparison["experiments"][0]
    assert group["status"] == "INCOMPATIBLE"
    assert group["reason"] == "both hdf5 and paimon results are required"
    assert group["metrics"] == {}


def test_load_results_combines_explicit_files_and_directory(tmp_path):
    experiment = {"schema_version": "act-benchmark-experiment@1"}
    environment = {"python": "3.10", "machine": "arm64"}
    hdf5_path = tmp_path / "hdf5.json"
    paimon_path = tmp_path / "paimon.json"
    ignored_path = tmp_path / "experiment.json"
    hdf5_path.write_text(json.dumps(
        _result("hdf5", experiment, environment, throughput=10.0)))
    paimon_path.write_text(json.dumps(
        _result("paimon", experiment, environment, throughput=15.0)))
    ignored_path.write_text(json.dumps(experiment))

    results = load_result_documents(
        [hdf5_path], results_dir=tmp_path)

    assert [result["backend"] for result in results] == ["hdf5", "paimon"]


def test_compare_groups_multiple_experiments_without_cross_comparing():
    environment = {"python": "3.10", "machine": "arm64"}
    results = []
    for seed in (1, 2):
        experiment = {
            "schema_version": "act-benchmark-experiment@1",
            "config": {"seed": seed},
        }
        results.extend([
            _result("hdf5", experiment, environment, throughput=10.0),
            _result("paimon", experiment, environment, throughput=15.0),
        ])

    comparison = compare_results(results)

    assert comparison["status"] == "SUCCEEDED"
    assert len(comparison["experiments"]) == 2
    assert all(
        group["backends"] == ["hdf5", "paimon"]
        for group in comparison["experiments"]
    )


def test_compare_reports_incompatible_runtime_environments():
    experiment = {"schema_version": "act-benchmark-experiment@1"}
    hdf5 = _result(
        "hdf5", experiment,
        {"python": "3.10", "machine": "arm64"}, throughput=10.0)
    paimon = _result(
        "paimon", experiment,
        {"python": "3.11", "machine": "arm64"}, throughput=15.0)

    comparison = compare_results([hdf5, paimon])

    assert comparison["status"] == "INCOMPATIBLE"
    group = comparison["experiments"][0]
    assert group["reason"] == "runtime environments differ"
    assert len(group["environment_sha256s"]) == 2
    assert group["metrics"] == {}


def test_compare_rejects_tampered_result_experiment_hash():
    experiment = {"schema_version": "act-benchmark-experiment@1"}
    result = _result(
        "hdf5",
        experiment,
        {"python": "3.10", "machine": "arm64"},
        throughput=10.0,
    )
    result["experiment_sha256"] = "tampered"

    with pytest.raises(ValueError, match="experiment SHA-256 differs"):
        compare_results([result])


def _require_act_runtime():
    pytest.importorskip("torch")
    pytest.importorskip("PIL.Image")


def _result(backend, experiment, environment, throughput):
    return {
        "schema_version": "act-benchmark-result@1",
        "status": "SUCCEEDED",
        "backend": backend,
        "experiment": experiment,
        "experiment_sha256": canonical_sha256(experiment),
        "environment": environment,
        "model": {"implementation": "test-policy", "parameter_count": 1},
        "tensor_fingerprint": {
            "sha256": "same-tensors",
            "checked_window_count": 2,
        },
        "runs": [{
            "round": round_number,
            "train_loss": [1.0, 0.5],
            "validation_loss": 0.25,
        } for round_number in range(1, 4)],
        "summary": {
            "round_count": 3,
            "batch_fetch_samples_per_s": {
                "median": throughput,
                "min": throughput,
                "max": throughput,
            },
        },
    }
