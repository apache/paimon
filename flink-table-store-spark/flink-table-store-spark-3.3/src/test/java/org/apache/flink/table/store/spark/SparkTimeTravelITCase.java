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

package org.apache.flink.table.store.spark;

import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.testutils.assertj.AssertionUtils;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for Spark time travel syntax (VERSION AS OF, TIMESTAMP AS OF). */
public class SparkTimeTravelITCase extends SparkTestBase {

    @Test
    public void testTravelToVersion() throws Exception {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        writeData(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        // snapshot 2
        writeData(
                "t",
                GenericRow.of(3, BinaryString.fromString("Test")),
                GenericRow.of(4, BinaryString.fromString("Case")));

        assertThat(spark.sql("SELECT * FROM t").collectAsList().toString())
                .isEqualTo("[[1,Hello], [2,Paimon], [3,Test], [4,Case]]");

        // time travel to snapshot 1
        assertThat(spark.sql("SELECT * FROM t VERSION AS OF 1").collectAsList().toString())
                .isEqualTo("[[1,Hello], [2,Paimon]]");
    }

    @Test
    public void testTravelToTimestampString() throws Exception {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        writeData(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        String anchor = LocalDateTime.now().toString();
        Thread.sleep(1000);

        // snapshot 2
        writeData(
                "t",
                GenericRow.of(3, BinaryString.fromString("Test")),
                GenericRow.of(4, BinaryString.fromString("Case")));

        assertThat(spark.sql("SELECT * FROM t").collectAsList().toString())
                .isEqualTo("[[1,Hello], [2,Paimon], [3,Test], [4,Case]]");

        // time travel to snapshot 1
        assertThat(
                        spark.sql(String.format("SELECT * FROM t TIMESTAMP AS OF '%s'", anchor))
                                .collectAsList()
                                .toString())
                .isEqualTo("[[1,Hello], [2,Paimon]]");
    }

    @Test
    public void testTravelToTimestampNumber() throws Exception {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        writeData(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        Thread.sleep(1000); // avoid precision problem
        long anchor = System.currentTimeMillis() / 1000; // convert to seconds
        Thread.sleep(1000);

        // snapshot 2
        writeData(
                "t",
                GenericRow.of(3, BinaryString.fromString("Test")),
                GenericRow.of(4, BinaryString.fromString("Case")));

        assertThat(spark.sql("SELECT * FROM t").collectAsList().toString())
                .isEqualTo("[[1,Hello], [2,Paimon], [3,Test], [4,Case]]");

        // time travel to snapshot 1
        assertThat(
                        spark.sql(String.format("SELECT * FROM t TIMESTAMP AS OF %s", anchor))
                                .collectAsList()
                                .toString())
                .isEqualTo("[[1,Hello], [2,Paimon]]");
    }

    @Test
    public void testTravelToOldSchema() throws Exception {
        // old schema
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        writeData(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        // new schema
        spark.sql("ALTER TABLE t ADD COLUMN dt STRING");

        // snapshot 2
        writeData(
                "t",
                GenericRow.of(3, BinaryString.fromString("Test"), BinaryString.fromString("0401")),
                GenericRow.of(4, BinaryString.fromString("Case"), BinaryString.fromString("0402")));

        // test that cannot see column dt
        assertThat(spark.sql("SELECT * FROM t VERSION AS OF 1").collectAsList().toString())
                .isEqualTo("[[1,Hello], [2,Paimon]]");
    }

    // ------------------------------------------------------------------------
    // Negative tests
    // ------------------------------------------------------------------------

    @Test
    public void testNonFileStoreTable() {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        assertThatThrownBy(() -> spark.sql("SELECT * FROM `t$files` VERSION AS OF 1"))
                .satisfies(
                        AssertionUtils.anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Only FileStoreTable supports time travel"));
    }

    @Test
    public void testIllegalVersionString() {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        assertThatThrownBy(() -> spark.sql("SELECT * FROM t VERSION AS OF '1.5'"))
                .satisfies(
                        AssertionUtils.anyCauseMatches(
                                IllegalArgumentException.class,
                                "Version for time travel should be a LONG value representing snapshot id but was '1.5'."));
    }

    @Test
    public void testTravelToNonExistedVersion() {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        assertThatThrownBy(() -> spark.sql("SELECT * FROM t VERSION AS OF 2"))
                .satisfies(
                        AssertionUtils.anyCauseMatches(
                                IllegalArgumentException.class,
                                "Target snapshot '2' for time travel doesn't exist."));
    }

    @Test
    public void testTravelToNonExistedTimestamp() throws Exception {
        long anchor = System.currentTimeMillis() / 1000;

        spark.sql("CREATE TABLE t (k INT, v STRING)");

        writeData(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        String.format(
                                                "SELECT * FROM t TIMESTAMP AS OF %s", anchor)))
                .satisfies(
                        AssertionUtils.anyCauseMatches(
                                RuntimeException.class,
                                "Time travel target snapshot id for timestamp"));
    }
}
