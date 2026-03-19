# Apache Paimon

Guidelines for AI coding assistants working with the Apache Paimon codebase.

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

- Single method: `mvn -pl <module> -Dtest=TestClassName#methodName test`
- Single class: `mvn -pl <module> -Dtest=TestClassName test`

#### Scala Tests (scalatest)

Scala tests use `scalatest-maven-plugin`, not surefire. Use `-DwildcardSuites` and `-Dtest=none`:

```shell
mvn -pl paimon-spark/paimon-spark-ut -am -Pfast-build -DfailIfNoTests=false \
  -DwildcardSuites=org.apache.paimon.spark.sql.WriteMergeSchemaTest \
  -Dtest=none test
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
