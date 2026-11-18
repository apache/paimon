#!/usr/bin/env python3
#
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

"""Run isolated, sequential Java spatial-index benchmark forks and retain raw results."""

import argparse
import csv
import datetime
import hashlib
import json
import math
import os
import pathlib
import platform
import re
import shlex
import statistics
import subprocess
import sys
import threading

MODULE = pathlib.Path(__file__).resolve().parent
REPO = MODULE.parent.parent
MAIN = "org.apache.paimon.benchmark.spatial.RTreeIndexBenchmark"


def capture(command, cwd=REPO):
    return subprocess.check_output(command, cwd=cwd, text=True, stderr=subprocess.STDOUT).strip()


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def summarize(raw, output):
    builds = []
    queries = []
    for record in raw:
        common = {key: record[key] for key in ("engine", "distribution", "rows", "capacity", "cache_mib", "io_latency_ms", "file_concurrency", "fork")}
        builds.append(dict(common, **{key: record[key] for key in (
            "files_written", "build_ms", "baseline_heap_mib", "build_peak_heap_mib", "build_peak_delta_mib",
            "retained_heap_delta_mib", "index_file_bytes", "process_peak_rss_mib")}))
        queries.extend(dict(common, **query) for query in record["queries"])
    write_csv(output / "builds.csv", builds)
    write_csv(output / "queries.csv", queries)
    for name, source, dimensions, metrics in [
        ("build-summary.csv", builds, ("engine", "distribution", "rows", "capacity", "cache_mib", "io_latency_ms", "file_concurrency"),
         ("files_written", "build_ms", "build_peak_heap_mib", "build_peak_delta_mib", "retained_heap_delta_mib", "index_file_bytes", "process_peak_rss_mib")),
        ("query-summary.csv", queries, ("engine", "distribution", "rows", "capacity", "cache_mib", "io_latency_ms", "file_concurrency", "window", "phase"),
         ("samples", "mean_us", "p50_us", "p95_us", "qps", "mean_matches", "reads_per_query", "bytes_per_query", "query_start_rss_mib", "query_peak_rss_mib", "query_peak_delta_mib", "rss_samples")),
    ]:
        grouped = {}
        for row in source:
            grouped.setdefault(tuple(row[key] for key in dimensions), []).append(row)
        summary = []
        for group, values in grouped.items():
            row = dict(zip(dimensions, group))
            row["forks"] = len(values)
            for metric in metrics:
                samples = [item[metric] for item in values if item.get(metric) is not None]
                row["median_" + metric] = statistics.median(samples) if samples else None
                row["min_" + metric] = min(samples) if samples else None
                row["max_" + metric] = max(samples) if samples else None
            summary.append(row)
        write_csv(output / name, summary)


def run_with_query_rss(command, timeout):
    """Handshake at phase boundaries so building, verification and priming cannot enter samples."""
    import psutil

    output = []
    phases = {}
    env = dict(os.environ, PAIMON_BENCHMARK_QUERY_RSS="1")
    with subprocess.Popen(command, cwd=REPO, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, env=env) as proc:
        root = psutil.Process(proc.pid)
        def terminate():
            for process in root.children(recursive=True):
                process.kill()
            proc.kill()
        timer = threading.Timer(timeout, terminate)
        timer.start()
        stop = threading.Event()
        sampler = None
        try:
            for line in proc.stdout:
                output.append(line)
                if line.startswith("QUERY_BEGIN "):
                    _, window, phase = line.split()
                    worker = root.children()[0] if command[0] == "/usr/bin/time" else root
                    initial = worker.memory_info().rss
                    peak = initial
                    samples = 1
                    def sample():
                        nonlocal peak, samples
                        while not stop.wait(0.002):
                            peak = max(peak, worker.memory_info().rss)
                            samples += 1
                    stop.clear()
                    sampler = threading.Thread(target=sample)
                    sampler.start()
                elif line.strip() == "QUERY_END":
                    stop.set()
                    sampler.join()
                    sampler = None
                    peak = max(peak, worker.memory_info().rss)
                    phases[window, phase] = {
                        "query_start_rss_mib": initial / 1048576,
                        "query_peak_rss_mib": peak / 1048576,
                        "query_peak_delta_mib": (peak - initial) / 1048576,
                        "rss_samples": samples + 1,
                    }
                else:
                    continue
                proc.stdin.write("\n")
                proc.stdin.flush()
            return subprocess.CompletedProcess(command, proc.wait(), "".join(output)), phases
        finally:
            stop.set()
            if sampler:
                sampler.join()
            timer.cancel()
            if proc.poll() is None:
                terminate()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", default="65536,1048576")
    parser.add_argument("--engines", help="Comma-separated engines; includes Rust when --rust-binary is set")
    parser.add_argument("--distributions", default="uniform-points,clustered-boxes,long-thin-boxes")
    parser.add_argument("--forks", type=int, default=3)
    parser.add_argument("--queries", type=int, default=32)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--capacity", type=int, default=0, help="0 uses each implementation's default")
    parser.add_argument("--cache-mib", type=int, default=8)
    parser.add_argument("--file-concurrencies", default="4", help="Comma-separated file concurrency limits for the R-tree readers")
    parser.add_argument("--sample-query-rss", action="store_true", help="Sample resident memory at query boundaries and every 2 ms (requires psutil)")
    parser.add_argument("--io-latencies-ms", default="0", help="Comma-separated per-range-read and per-output-file delays on the file engines")
    parser.add_argument("--java", default="java")
    parser.add_argument("--jvm-args", default="-Xms256m -Xmx1g -XX:+UseSerialGC -XX:ActiveProcessorCount=4")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--rust-binary", type=pathlib.Path)
    parser.add_argument("--rust-root", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    row_counts = [int(value) for value in args.rows.split(",")]
    engines = (args.engines or "paimon-memory,paimon-file,jts-hpr,jts-str" +
               (",rust-memory,rust-file" if args.rust_binary else "")).split(",")
    if any(engine.startswith("rust-") for engine in engines) and not args.rust_binary:
        parser.error("Rust engines require --rust-binary")
    latencies = [int(value) for value in args.io_latencies_ms.split(",")]
    if min(latencies) < 0 or len(set(latencies)) != len(latencies):
        parser.error("IO latencies must be distinct non-negative milliseconds")
    limits = [int(value) for value in args.file_concurrencies.split(",")]
    if min(limits) < 1 or len(set(limits)) != len(limits):
        parser.error("File concurrency limits must be distinct positive integers")
    scenarios = [(engine, delay, limit) for delay in latencies for limit in limits for engine in engines
                 if (engine in ("paimon-file", "rust-file") or delay == 0)
                 and (not engine.startswith("jts-") or limit == limits[0])]
    if not scenarios:
        parser.error("Positive IO latencies require file engines")
    distributions = args.distributions.split(",")
    if min(row_counts + [args.forks, args.queries, args.rounds]) < 1:
        parser.error("rows, forks, queries and rounds must be positive")
    output = (args.output or MODULE / "target" / "spatial-benchmark" /
              datetime.datetime.now().strftime("%Y%m%d-%H%M%S")).resolve()
    output.mkdir(parents=True, exist_ok=False)
    build_command = [
        "mvn", "-pl", "paimon-benchmark/paimon-micro-benchmarks", "-am",
        "-DskipTests", "test-compile", "dependency:build-classpath",
        "-Dmdep.outputFile=target/spatial-benchmark-classpath",
        "-DincludeScope=test",
    ]
    if not args.skip_build:
        print("Building benchmark; log: " + str(output / "build.log"), flush=True)
        with (output / "build.log").open("w") as log:
            subprocess.run(build_command, cwd=REPO, stdout=log, stderr=subprocess.STDOUT, check=True)
    dependency_cp = MODULE / "target" / "spatial-benchmark-classpath"
    # Put current worktree class directories before any installed/shaded Paimon jars.
    cp = os.pathsep.join(str(path) for path in [
        MODULE / "target" / "test-classes", MODULE / "target" / "classes",
        REPO / "paimon-api" / "target" / "classes",
        REPO / "paimon-common" / "target" / "classes",
    ]) + os.pathsep + dependency_cp.read_text().strip()
    sources = list((REPO / "paimon-common/src/main/java/org/apache/paimon/globalindex/rtree").glob("*.java"))
    sources += list((REPO / "paimon-common/src/main/java/org/apache/paimon/geo").glob("*.java"))
    sources += [MODULE / "src/test/java/org/apache/paimon/benchmark/spatial/RTreeIndexBenchmark.java",
                pathlib.Path(__file__).resolve()]
    metadata = {
        "arguments": vars(args).copy(),
        "utc_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "git_head": capture(["git", "rev-parse", "HEAD"]),
        "git_status": capture(["git", "status", "--short"]),
        "java": capture([args.java, "-version"]),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "jts_version": "1.20.0",
        "jvm_args": shlex.split(args.jvm_args),
        "source_sha256": {str(path.relative_to(REPO)): hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in sources},
    }
    if platform.system() == "Darwin":
        metadata["cpu"] = capture(["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"])
        metadata["physical_memory_bytes"] = int(capture(["/usr/sbin/sysctl", "-n", "hw.memsize"]))
    metadata["arguments"]["output"] = str(output)
    metadata["arguments"]["rust_binary"] = str(args.rust_binary) if args.rust_binary else None
    metadata["arguments"]["rust_root"] = str(args.rust_root) if args.rust_root else None
    metadata["rss_scope"] = "whole worker process including warmups, build and queries; OS high-water RSS"
    if args.rust_root:
        metadata["rust_git_head"] = capture(["git", "rev-parse", "HEAD"], args.rust_root)
        metadata["rust_git_status"] = capture(["git", "status", "--short"], args.rust_root)
        metadata["rustc"] = capture(["rustc", "-vV"], args.rust_root)
        metadata["rust_source_sha256"] = {
            str(path.relative_to(args.rust_root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in list((args.rust_root / "crates/paimon/src/rtree").rglob("*.rs")) +
            [args.rust_root / "crates/paimon/examples/rtree_index_benchmark.rs"]}
    if args.rust_binary:
        metadata["rust_binary_sha256"] = hashlib.sha256(args.rust_binary.read_bytes()).hexdigest()
    workloads = {}
    if args.rust_binary:
        (output / "workloads").mkdir()
        for rows in row_counts:
            for distribution in distributions:
                path = output / "workloads" / f"{rows}-{distribution}.bin"
                subprocess.run([args.java, "-cp", cp, MAIN, "--export", distribution,
                                str(rows), str(args.queries), str(path)], cwd=REPO, check=True)
                workloads[rows, distribution] = path
        metadata["workload_sha256"] = {
            str(path.name): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in workloads.values()}
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    records = []
    reference_matches = {}
    reference_io = {}
    reference_files = {}
    count = 0
    total = len(row_counts) * len(distributions) * args.forks * len(scenarios)
    try:
        for rows in row_counts:
            for distribution in distributions:
                for fork in range(args.forks):
                    # Rotate order to reduce systematic temperature/load/order bias.
                    order = scenarios[fork % len(scenarios):] + scenarios[:fork % len(scenarios)]
                    if fork % 2:
                        order = list(reversed(order))
                    for engine, delay, limit in order:
                        count += 1
                        label = f"{rows}-{distribution}-{engine}-{delay}ms-c{limit}-fork{fork}"
                        print(f"[{count}/{total}] {label}", flush=True)
                        if engine.startswith("rust-"):
                            command = [str(args.rust_binary.resolve()), engine,
                                       str(workloads[rows, distribution]), str(args.rounds),
                                       str(args.capacity), str(args.cache_mib), str(delay), "65536", str(limit)]
                        else:
                            command = [args.java] + shlex.split(args.jvm_args) + [
                                "-cp", cp, MAIN, engine, distribution, str(rows),
                                str(args.queries), str(args.rounds), str(args.capacity), str(args.cache_mib), str(delay), str(limit)]
                        rss_file = output / (label + ".rss")
                        system = platform.system()
                        if system in ("Darwin", "Linux") and pathlib.Path("/usr/bin/time").exists():
                            command = ["/usr/bin/time", "-l" if system == "Darwin" else "-v",
                                       "-o", str(rss_file)] + command
                        timeout = max(900, 900 + delay * args.queries * args.rounds * 6)
                        phase_rss = {}
                        if args.sample_query_rss:
                            completed, phase_rss = run_with_query_rss(command, timeout)
                        else:
                            completed = subprocess.run(command, cwd=REPO, text=True, stdout=subprocess.PIPE,
                                                       stderr=subprocess.STDOUT, timeout=timeout)
                        (output / (label + ".log")).write_text(completed.stdout)
                        if completed.returncode:
                            raise RuntimeError("Worker failed: " + str(output / (label + ".log")))
                        lines = [line[7:] for line in completed.stdout.splitlines() if line.startswith("RESULT ")]
                        if len(lines) != 1:
                            raise RuntimeError("Missing unique result: " + label)
                        result = json.loads(lines[0])
                        if result["io_latency_ms"] != delay or result["file_concurrency"] != limit:
                            raise RuntimeError("Unexpected worker latency: " + label)
                        result["distribution"] = distribution
                        result["process_peak_rss_mib"] = None
                        if rss_file.exists():
                            rss = rss_file.read_text()
                            if system == "Darwin":
                                value = re.search(r"(\d+)\s+maximum resident set size", rss)
                                scale = 1
                            else:
                                value = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", rss)
                                scale = 1024
                            if value:
                                result["process_peak_rss_mib"] = int(value.group(1)) * scale / 1048576
                        result["fork"] = fork
                        if engine in ("paimon-file", "rust-file"):
                            sizes = result["files_written"], result["index_file_bytes"]
                            if reference_files.setdefault((rows, distribution), sizes) != sizes:
                                raise RuntimeError("Serialized index sizes differ: " + label)
                        for query in result["queries"]:
                            if args.sample_query_rss:
                                query.update(phase_rss[query["window"], query["phase"]])
                            # Cold requests match across readers. Warm traffic also depends on
                            # how each reader charges shared page buffers to its cache.
                            if engine in ("paimon-file", "rust-file") and (limit == 1 or query["phase"] == "cache-cold"):
                                reader = engine if query["phase"] == "cache-warm" else None
                                io_key = rows, distribution, query["window"], query["phase"], reader
                                io = query["reads_per_query"], query["bytes_per_query"]
                                if reference_io.setdefault(io_key, io) != io:
                                    raise RuntimeError("Range reads/bytes differ: " + label)
                            key = rows, distribution, query["window"]
                            reference = reference_matches.setdefault(key, query["mean_matches"])
                            if not math.isclose(reference, query["mean_matches"], rel_tol=1e-12):
                                raise RuntimeError("Cross-implementation match counts differ: " + label)
                        records.append(result)
                        (output / "raw.json").write_text(json.dumps(records, indent=2) + "\n")
                        summarize(records, output)
    finally:
        print("Results: " + str(output), flush=True)


if __name__ == "__main__":
    main()
