# Apache Paimon

Guidelines for AI coding assistants working with the Apache Paimon codebase.

## Architecture and Table Modes

Identify the table mode before explaining storage, reads, updates, or compaction. Paimon shares
schema, snapshot, and manifest metadata across different data layouts; it must not be described
as universally LSM-based.

| Mode | Data organization and read semantics |
| --- | --- |
| Ordinary append table | Stores inserted rows without primary-key merging. |
| Primary-key table | Uses an LSM-based layout to reconcile record versions by key. |
| Data Evolution append table | Aligns column files by row-ID range and selects current column values by file version, independently of primary-key LSM merging. |

Data Evolution requires `data-evolution.enabled=true`, `row-tracking.enabled=true`, no primary
key, and `bucket=-1`. It supports partial-column updates that retain files for untouched columns.
Its column merging and compaction must not be explained as primary-key LSM merging. A
`bucket-0` directory, sequence-number metadata, or compaction alone does not imply an LSM layout.

For architecture and file-layout comparisons, cover Data Evolution explicitly rather than
using the primary-key layout as the model for all Paimon tables.
Distinguish table metadata and row reconstruction from the configurable physical file format.

Start with [Basic Concepts](docs/docs/concepts/basic-concepts.md),
[Data Evolution](docs/docs/multimodal-table/data-evolution.mdx), and
[File Layout and Reads](docs/docs/multimodal-table/data-evolution-file-layout.mdx).
Verify implementation details in `AppendOnlyFileStoreTable`, `DataEvolutionSplitRead`,
`DataEvolutionReadPlanner`, and `SchemaValidation` under `paimon-core/src/main/java/org/apache/paimon/`.

## Syntax Requirements

- Based on JDK 8 and Scala 2.12, higher version syntax features must not be used.

## Build and Test

Prefer the smallest possible build/test scope and use the speedup techniques below to keep feedback loops fast.

### Build

- Compile a single module: `mvn -pl <module> -DskipTests compile`
- Compile multiple modules: `mvn -pl <module1>,<module2> -DskipTests compile`

### Test

Prefer running the narrowest tests.

#### Java Tests (surefire)

- Single method: `mvn -pl <module> -DwildcardSuites=none -Dtest=TestClassName#methodName test`
- Single class: `mvn -pl <module> -DwildcardSuites=none -Dtest=TestClassName test`

#### Scala Tests (scalatest)

Scala tests use `scalatest-maven-plugin`, not surefire. Use `-DwildcardSuites` and `-Dtest=none`:

```shell
mvn -pl paimon-spark/paimon-spark-ut -am -Pfast-build -DfailIfNoTests=false \
  -DwildcardSuites=org.apache.paimon.spark.sql.WriteMergeSchemaTest \
  -Dtest=none test
```

#### Required Maven Profiles
- Spark tests: add `-Pspark3` or `-Pspark4`
- Flink tests: add `-Pflink1` or `-Pflink2`

For example, run a Java test in the Spark UT with Spark 3:
```shell
mvn -pl paimon-spark/paimon-spark-ut -am -Pfast-build -Pspark3 \
  -DwildcardSuites=none \
  -DfailIfNoTests=false \
  -Dtest=SparkChainTableITCase#testChainTable test
```

### Local Iteration Speedup

Use `-Pfast-build` to skip checkstyle, spotless, enforcer, and rat checks (not for final verification):

```shell
mvn -pl <module> -Pfast-build -Dtest=TestClassName#methodName test
```

Use `-am` if your target module depends on locally changed modules. Add `-DfailIfNoTests=false` to avoid failures for modules without tests:

```shell
mvn -pl <module> -am -Pfast-build -DfailIfNoTests=false -Dtest=TestClassName#methodName test
```
