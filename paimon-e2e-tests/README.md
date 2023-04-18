<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# End-to-End (E2E) Testing
This directory contains end-to-end (e2e) testing for Paimon, which ensures users can run Paimon as expected across the entire stack.

## Run e2e tests Locally
Currently, e2e supports docker environment only. You need to do some preparations before running e2e tests.

### Prepare
1. Install docker engine or desktop in your local environment.
2. Start docker daemon.
3. Set `rootLogger.level` to `INFO` at `paimon-e2e-tests/src/test/resources/log4j2-test.properties` if you need to debug. 

### Execution
1. Build with Flink-versioned profiles, like flink-1.17. The default Flink main version is declared in the parent `pom.xml` by `<test.flink.main.version>` tag.

```Bash
mvn clean install -DskipTests -Pflink-1.17
```
2. Run e2e tests
```Bash
mvn test -pl paimon-e2e-tests -Pflink-1.17
```