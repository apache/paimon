# Paimon Benchmark

This is the cluster benchmark module for Paimon. Inspired by [Nexmark](https://github.com/nexmark/nexmark).

## How To Run
### Environment Preparation
* This benchmark only runs on Linux. You'll need a Linux environment (preferably an EMR cluster). For a more reasonable result, we recommend this cluster to have:
  * One master node with 8 cores and 16GB RAM.
  * Two worker nodes with 16 cores and 64GB RAM.
* This benchmark runs on a standalone Flink cluster. Download Flink >= 1.15 from the [Apache Flink's website](https://flink.apache.org/downloads.html#apache-flink-1160) and set up a standalone cluster. Flink's job manager must be on the master node of your EMR cluster. We recommend the following Flink configurations:
    ```yaml
    jobmanager.memory.process.size: 4096m
    taskmanager.memory.process.size: 4096m
    taskmanager.numberOfTaskSlots: 1
    parallelism.default: 16
    execution.checkpointing.interval: 3min
    state.backend: rocksdb
    state.backend.incremental: true
    ```
    With this Flink configuration, you'll need 16 task manager instances in total, 8 on each EMR worker.
* This benchmark needs the `FLINK_HOME` environment variable. Set `FLINK_HOME` to your Flink directory.

### Setup Benchmark
1. Build this module with command `mvn clean package -DskipTests -pl paimon-benchmark/paimon-cluster-benchmark/ -am`.
2. Copy `paimon-benchmark/paimon-cluster-benchmark/target/paimon-benchmark-bin/paimon-benchmark` to the master node of your EMR cluster.
3. Modify `paimon-benchmark/conf/benchmark.yaml` on the master node. You must change these config options:
   * `benchmark.metric.reporter.host` and `flink.rest.address`: set these to the address of master node of your EMR cluster.
   * `benchmark.sink.path` is the path to which queries insert records. This should point to a non-existing path. Contents of this path will be removed before each test.
4. Copy `paimon-benchmark` to every worker node of your EMR cluster.
5. Run `paimon-benchmark/bin/setup_cluster.sh` in master node. This activates the CPU metrics collector in worker nodes. Note that if you restart your Flink cluster, you must also restart the CPU metrics collectors. To stop CPU metrics collectors, run `paimon-benchmark/bin/shutdown_cluster.sh` in master node.

### Run Benchmark
* Run `paimon-benchmark/bin/run_benchmark.sh <query> <sink>` to run `<query>` for `<sink>`. Currently `<query>` can be `q1` or `all`, and sink can only be `paimon`.
* By default, each query writes for 30 minutes and then reads all records back from the sink to measure read throughput.

## Queries

|#|Description|
|---|---|
|q1|Test insert and update random primary keys with normal record size (150 bytes per record). Mimics the update of uv and pv of items in an E-commercial website.|

## Benchmark Results

Results of each query consist of the following aspects:
* Throughput (rows/s): Average number of rows inserted into the sink per second.
* Total Rows: Total number of rows written.
* Cores: Average CPU cost.
* Throughput/Cores: Number of bytes inserted into the sink per second per CPU.
* Avg Data Freshness: Average time elapsed from the starting point of the last successful checkpoint.
* Max Data Freshness: Max time elapsed from the starting point of the last successful checkpoint.

## How to Add New Queries
1. Add your query to `paimon-benchmark/queries` as a SQL script.
2. Modify `paimon-benchmark/queries/queries.yaml` to set the properties of this test. Supported properties are:
  * `sql`: An array of SQL scripts. These SQL scripts will run in the given order. Each SQL script will stop only after it produces enough number of rows.
  * `row-num`: Number of rows produced from the source for each SQL scripts.

Note that each query must contain a `-- __SINK_DDL_BEGIN__` and `-- __SINK_DDL_END__` so that this DDL can be used for both write and read tests. See existing queries for detail.

## How to Add New Sinks
Just add your sink to `paimon-benchmark/sinks` as a yaml file, which supports the following properties:
  * `before`: A SQL script which is appended before each test. This property is useful if your sink needs initialization (for example if your sink needs a special catalog).
  * `sink-name`: Name of the sink table. This property will replace the `${SINK_NAME}` in each query.
  * `sink-properties`: The `WITH` option of the sink. This property will replace the `${DDL_TEMPLATE}` in each query.

See existing sinks for detail.
