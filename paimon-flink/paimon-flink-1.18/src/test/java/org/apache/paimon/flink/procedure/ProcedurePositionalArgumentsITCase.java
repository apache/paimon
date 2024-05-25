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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.CatalogITCaseBase;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

/** Ensure that the legacy multiply overloaded CALL with positional arguments can be invoked. */
public class ProcedurePositionalArgumentsITCase extends CatalogITCaseBase {

    @Test
    public void testCallCompact() {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " pt INT,"
                        + " PRIMARY KEY (k, pt) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");

        assertThatCode(() -> sql("CALL sys.compact('default.T')")).doesNotThrowAnyException();
        assertThatCode(() -> sql("CALL sys.compact('default.T', 'pt=1')"))
                .doesNotThrowAnyException();
        assertThatCode(() -> sql("CALL sys.compact('default.T', 'pt=1', '', '')"))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                sql(
                                        "CALL sys.compact('default.T', '', '', '', 'sink.parallelism=1','pt=1')"))
                .doesNotThrowAnyException();
    }
}
