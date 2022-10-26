# flink-table-store-micro-benchmarks

This project contains sets of micro benchmarks designed to run on a single machine to
help flink table store developers assess performance implications of their changes.

The main methods defined in the various classes are using [jmh](http://openjdk.java.net/projects/code-tools/jmh/) micro
benchmark suite to define runners to execute those cases. You can execute the
default benchmark suite at once as follows:

1. Build and install in `flink-table-store` project.
```
mvn clean install -DskipTests
```
2. Build and run micro benchmarks in `flink-table-store-micro-benchmarks` project.
```
mvn clean install -DskipTests exec:exec
```

If you want to execute just one benchmark, the best approach is to execute selected main function manually.
There are mainly three ways:

1. From your IDE (hint there is a plugin for Intellij IDEA).

2. From command line, using command like:
   ```
   mvn clean install -DskipTests exec:exec \
    -Dbenchmarks="<benchmark_class>"
   ```

   An example benchmark_class can be `MergeTreeReaderBenchmark` to measure the performance of merge tree reader.

3. Run the uber jar directly like:

    ```
    java -jar target/benchmarks.jar -rf csv "<benchmark_class>"
    ```

## Configuration

Besides the parameters, there is also a benchmark config file `benchmark-conf.yaml` to tune some basic parameters.
For example, we can change the file data dir by putting `benchmark.file.base-data-dir: /data` in the config file. For more options, you can refer to the options in `FileBenchmarkOptions` and flink table store. 
