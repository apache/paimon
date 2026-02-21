# Apache Paimon

This file provides context and guidelines for AI coding assistants working with the Apache Paimon codebase.

## Syntax Requirements

- Based on JDK 8 and Scala 2.12, higher version syntax features must not be used.

## Build and Test

Prefer the smallest possible build/test scope and use the local iteration speedup techniques below to keep feedback loops fast.

### Build

Prefer using Maven module selection to minimize the build scope.

- Compile a single module: `mvn -pl <module> -DskipTests compile`
- Compile multiple modules: `mvn -pl <module1>,<module2> -DskipTests compile`

### Test

Prefer running the narrowest tests.

- Run test for a single method: `mvn -pl <module> -Dtest=TestClassName#methodName test`
- Run test for a single class: `mvn -pl <module> -Dtest=TestClassName test`

### Local Iteration Speedup

#### Use Maven Skip Options

Prefer these skip options for faster local iteration, but don't rely on them for final verification.

```shell
-Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip
```

For example:

```shell
mvn -pl <module> -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip -Dtest=TestClassName#methodName test
```

#### Use -am (also-make)

If your target module depends on other modules you've changed locally, prefer `-am` to rebuild them together in the same reactor.
Also add `-DfailIfNoTests=false` to avoid failures for modules without tests.

For example:

```shell
mvn -pl <module> -am -DfailIfNoTests=false -Dtest=TestClassName#methodName test
```
