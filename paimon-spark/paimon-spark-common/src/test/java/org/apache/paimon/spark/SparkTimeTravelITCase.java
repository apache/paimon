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

package org.apache.paimon.spark;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for Spark 3.3+ time travel syntax (VERSION AS OF, TIMESTAMP AS OF). */
public class SparkTimeTravelITCase extends SparkReadTestBase {

    @Test
    public void testTravelToVersion() throws Exception {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        writeTable(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        // snapshot 2
        writeTable(
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
        writeTable(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        String anchor = LocalDateTime.now().toString();

        // snapshot 2
        writeTable(
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
        writeTable(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        Thread.sleep(1000); // avoid precision problem
        long anchor = System.currentTimeMillis() / 1000; // convert to seconds

        // snapshot 2
        writeTable(
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
        writeTable(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        // new schema
        spark.sql("ALTER TABLE t ADD COLUMN dt STRING");

        // snapshot 2
        writeTable(
                "t",
                GenericRow.of(3, BinaryString.fromString("Test"), BinaryString.fromString("0401")),
                GenericRow.of(4, BinaryString.fromString("Case"), BinaryString.fromString("0402")));

        assertThat(spark.sql("SELECT * FROM t").collectAsList().toString())
                .isEqualTo("[[1,Hello,null], [2,Paimon,null], [3,Test,0401], [4,Case,0402]]");

        // test that cannot see column dt after time travel
        assertThat(spark.sql("SELECT * FROM t VERSION AS OF 1").collectAsList().toString())
                .isEqualTo("[[1,Hello], [2,Paimon]]");
    }

    @Test
    public void testTravelToNonExistedVersion() {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        assertThat(spark.sql("SELECT * FROM t VERSION AS OF 2").collectAsList()).isEmpty();
    }

    @Test
    public void testTravelToNonExistedTimestamp() {
        long anchor = System.currentTimeMillis() / 1000;

        spark.sql("CREATE TABLE t (k INT, v STRING)");

        assertThat(
                        spark.sql(String.format("SELECT * FROM t TIMESTAMP AS OF %s", anchor))
                                .collectAsList())
                .isEmpty();
    }

    @Test
    public void testSystemTableTimeTravel() throws Exception {
        spark.sql("CREATE TABLE t (k INT, v STRING) TBLPROPERTIES ('bucket' = '1')");

        // snapshot 1
        writeTable(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        String anchor = LocalDateTime.now().toString();

        // snapshot 2
        writeTable(
                "t",
                GenericRow.of(3, BinaryString.fromString("Test")),
                GenericRow.of(4, BinaryString.fromString("Case")));

        assertThat(spark.sql("SELECT * FROM `t$files`").collectAsList().size()).isEqualTo(2);

        // time travel to snapshot 1
        assertThat(spark.sql("SELECT * FROM `t$files` VERSION AS OF 1").collectAsList().size())
                .isEqualTo(1);
        assertThat(
                        spark.sql(
                                        String.format(
                                                "SELECT * FROM `t$files` TIMESTAMP AS OF '%s'",
                                                anchor))
                                .collectAsList()
                                .size())
                .isEqualTo(1);
    }

    @Test
    public void testTravelToTag() throws Exception {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        writeTable(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        // snapshot 2
        writeTable(
                "t",
                GenericRow.of(3, BinaryString.fromString("Test")),
                GenericRow.of(4, BinaryString.fromString("Case")));

        // snapshot 3
        writeTable(
                "t",
                GenericRow.of(5, BinaryString.fromString("Time")),
                GenericRow.of(6, BinaryString.fromString("Travel")));

        getTable("t").createTag("tag2", 2);

        // time travel to tag2
        assertThat(spark.sql("SELECT * FROM t VERSION AS OF 'tag2'").collectAsList().toString())
                .isEqualTo("[[1,Hello], [2,Paimon], [3,Test], [4,Case]]");
    }

    @Test
    public void testTravelToNonExistingTag() {
        spark.sql("CREATE TABLE t (k INT, v STRING)");
        assertThatThrownBy(
                        () -> spark.sql("SELECT * FROM t VERSION AS OF 'unknown'").collectAsList())
                .satisfies(
                        anyCauseMatches(
                                RuntimeException.class,
                                "Cannot find a time travel version for unknown"));
    }

    @Test
    public void testTravelToTagWithSnapshotExpiration() throws Exception {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        writeTable(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        // snapshot 2
        writeTable(
                "t",
                GenericRow.of(3, BinaryString.fromString("Test")),
                GenericRow.of(4, BinaryString.fromString("Case")));

        // snapshot 3
        writeTable(
                "t",
                GenericRow.of(5, BinaryString.fromString("Time")),
                GenericRow.of(6, BinaryString.fromString("Travel")));

        FileStoreTable table = getTable("t");
        table.createTag("tag2", 2);

        // expire snapshot 1 & 2
        Map<String, String> expireOptions = new HashMap<>();
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        table.copy(expireOptions).newCommit("").expireSnapshots();
        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(1);

        // time travel to tag2
        assertThat(spark.sql("SELECT * FROM t VERSION AS OF 'tag2'").collectAsList().toString())
                .isEqualTo("[[1,Hello], [2,Paimon], [3,Test], [4,Case]]");
    }

    @Test
    public void testTravelToTagWithDigitalName() throws Exception {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        writeTable(
                "t",
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        // snapshot 2
        writeTable(
                "t",
                GenericRow.of(3, BinaryString.fromString("Test")),
                GenericRow.of(4, BinaryString.fromString("Case")));

        FileStoreTable table = getTable("t");
        table.createTag("1", 2);

        // time travel to tag '1'
        assertThat(spark.sql("SELECT * FROM t VERSION AS OF '1'").collectAsList().toString())
                .isEqualTo("[[1,Hello], [2,Paimon], [3,Test], [4,Case]]");
    }

    @Test
    public void testTravelWithWatermark() throws Exception {
        spark.sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        writeTableWithWatermark(
                "t",
                1L,
                GenericRow.of(1, BinaryString.fromString("Hello")),
                GenericRow.of(2, BinaryString.fromString("Paimon")));

        // snapshot 2
        writeTableWithWatermark(
                "t",
                null,
                GenericRow.of(1, BinaryString.fromString("Null")),
                GenericRow.of(2, BinaryString.fromString("Watermark")));

        // snapshot 3
        writeTableWithWatermark(
                "t",
                10L,
                GenericRow.of(3, BinaryString.fromString("Time")),
                GenericRow.of(4, BinaryString.fromString("Travel")));

        // time travel to watermark '1'
        assertThat(
                        spark.sql("SELECT * FROM t version as of 'watermark-1'")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[1,Hello], [2,Paimon]]");

        try {
            spark.sql("SELECT * FROM t version as of 'watermark-11'").collectAsList();
        } catch (Exception e) {
            assertThat(
                    e.getMessage()
                            .equals(
                                    "There is currently no snapshot later than or equal to watermark[11]"));
        }

        // time travel to watermark '9'
        assertThat(
                        spark.sql("SELECT * FROM t version as of 'watermark-9'")
                                .collectAsList()
                                .toString())
                .isEqualTo(
                        "[[1,Hello], [2,Paimon], [1,Null], [2,Watermark], [3,Time], [4,Travel]]");

        // time travel to watermark '10'
        assertThat(
                        spark.sql("SELECT * FROM t version as of 'watermark-10'")
                                .collectAsList()
                                .toString())
                .isEqualTo(
                        "[[1,Hello], [2,Paimon], [1,Null], [2,Watermark], [3,Time], [4,Travel]]");
    }
}
