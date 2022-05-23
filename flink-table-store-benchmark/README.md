# Flink Table Store Benchmark

This is the benchmark module for Flink Table Store. Inspired by [Nexmark](https://github.com/nexmark/nexmark).

## How To Run
### Environment Preparation
* This benchmark only runs on Linux. You'll need a Linux environment (preferably an EMR cluster).
* This benchmark runs on a standalone Flink cluster. Download Flink >= 1.15 from the [Apache Flink's website](https://flink.apache.org/downloads.html#apache-flink-1150) and setup a standalone cluster. Flink's job manager must be on the master node of your EMR cluster.
* This benchmark needs the `FLINK_HOME` environment variable. Set `FLINK_HOME` to your Flink directory.

### Setup Benchmark
* Build this module with command `mvn clean package`.
* Copy `target/flink-table-store-benchmark-bin/flink-table-store-benchmark` to the master node of your EMR cluster.
* Modify `flink-table-store-benchmark/conf/benchmark.yaml` on the master node. You must change these config options:
  * `benchmark.metric.reporter.host` and `flink.rest.address`: set these to the address of master node of your EMR cluster.
  *  `benchmark.sink.path` is the path to which queries insert records. This should point to a non-existing path. Contents of this path will be removed before each test.
* Copy `flink-table-store-benchmark` to every worker node of your EMR cluster.
* Run `flink-table-store-benchmark/bin/setup_cluster.sh` in master node. This activates the CPU metrics collector in worker nodes. Note that if you restart your Flink cluster, you must also restart the CPU metrics collectors. To stop CPU metrics collectors, run `flink-table-store-benchmark/bin/shutdown_cluster.sh` in master node.

### Run Benchmark
* Run `flink-table-store-benchmark/bin/run_benchmark.sh <query> <sink>` to run `<query>` for `<sink>`. Currently `<query>` can be `q1`, `q2` or `all`, and sink can only be `table_store`.
* By default, each query writes for 30 minutes (with a checkpoint interval of 3 minutes) and then reads all records back from the sink to measure read throughput. 

## Queries

|#|Description|
|---|---|
|q1|Mimics the insert and update of orders in an E-commercial website.<br><br>Primary keys are totally random; Each record is about 1.5K bytes.|
|q2|Mimics the update of uv and pv by hour of items in an E-commercial website.<br><br>Primary keys are related with real time; Each record is about 100 bytes.|

## Benchmark Results

Results of each query consist of the following aspects:
* Throughput (byte/s): Average number of bytes inserted into the sink per second.
* Total Bytes: Total number of bytes written during the given time.
* Cores: Average CPU cost.
* Throughput/Cores: Number of bytes inserted into the sink per second per CPU.
* Avg Data Freshness: Average time elapsed from the starting point of the last successful checkpoint.
* Max Data Freshness: Max time elapsed from the starting point of the last successful checkpoint.
* Query Throughput (row/s): Number of rows read from the sink per second.

## How to Add New Queries
Just add your query to `flink-table-store-benchmark/queries` as a SQL script. Note that each query must contain a `-- __SINK_DDL_BEGIN__` and `-- __SINK_DDL_END__` so that this DDL can be used for both write and read tests. See existing queries for detail.

## How to Add New Sinks
Just add your sink to `flink-table-store-benchmark/sinks` as a SQL script. This SQL script should only contain the `WITH` options of that sink. See existing sinks for detail.