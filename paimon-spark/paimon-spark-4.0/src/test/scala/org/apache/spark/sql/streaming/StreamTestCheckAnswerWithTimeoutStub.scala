/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.streaming

/**
 * Test-scope stubs for Spark 4.1-only `StreamTest` inner classes.
 *
 * Spark 4.1 added several `CheckAnswer` / `WaitUntil` helpers inside `StreamTest`; Paimon's shared
 * test base classes in `paimon-spark-ut` mix in `StreamTest` and are compiled against Spark 4.1.1
 * (the default `spark.version` under the `spark4` profile), so their compiled bytecode has
 * constant-pool references to the nested companion objects.
 *
 * When those test classes are loaded on a Spark 4.0.2 runtime (the paimon-spark-4.0 test suite),
 * `junit-vintage`'s discovery phase tries to verify each class and fails to link the missing 4.1
 * symbols, crashing the engine before any Paimon test can run. No Paimon test actually invokes
 * these helpers — the references only exist in the constant pool from trait-mix-in synthetic
 * forwarders — so empty class files with the matching names are enough to satisfy the verifier.
 *
 * Diffing `spark-sql_2.13-4.1.1-tests.jar` against `spark-sql_2.13-4.0.1-tests.jar` shows the
 * following `StreamTest$…` classes are 4.1-only:
 *
 * companion objects (always referenced via MODULE$ access):
 *   - `StreamTest$CheckAnswerWithTimeout$`
 *   - `StreamTest$CheckAnswerRowsContainsWithTimeout$`
 *   - `StreamTest$CheckAnswerRowsNoWait$`
 *   - `StreamTest$WaitUntilBatchProcessed$`
 *   - `StreamTest$WaitUntilCurrentBatchProcessed$`
 *
 * case classes (may be referenced as parameter / return types):
 *   - `StreamTest$CheckAnswerRowsContainsWithTimeout`
 *   - `StreamTest$CheckAnswerRowsNoWait`
 *   - `StreamTest$WaitUntilBatchProcessed`
 *
 * Scala's backtick syntax lets us declare classes whose compiled names contain `$`, so each stub
 * compiles to exactly the file name expected by the verifier. They live in `src/test/scala/` so
 * they are never packaged into any production jar (including the paimon-spark-4.0 shaded artifact)
 * and do not leak onto Spark 4.1 classpaths where the real symbols must win.
 */
private[streaming] class `StreamTest$CheckAnswerWithTimeout$`
private[streaming] class `StreamTest$CheckAnswerRowsContainsWithTimeout$`
private[streaming] class `StreamTest$CheckAnswerRowsNoWait$`
private[streaming] class `StreamTest$WaitUntilBatchProcessed$`
private[streaming] class `StreamTest$WaitUntilCurrentBatchProcessed$`

private[streaming] class `StreamTest$CheckAnswerRowsContainsWithTimeout`
private[streaming] class `StreamTest$CheckAnswerRowsNoWait`
private[streaming] class `StreamTest$WaitUntilBatchProcessed`
