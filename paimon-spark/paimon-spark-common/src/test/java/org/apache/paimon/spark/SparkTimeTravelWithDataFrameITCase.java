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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for Spark time travel with DataFrames. */
public class SparkTimeTravelWithDataFrameITCase extends SparkReadTestBase {

    @Test
    public void testTravelToVersion() throws Exception {
        // snapshot 2
        writeTable(
                "t1",
                GenericRow.of(7, 2L, BinaryString.fromString("7")),
                GenericRow.of(8, 4L, BinaryString.fromString("8")));

        Dataset<Row> dataset =
                spark.read()
                        .format("paimon")
                        .option("path", tablePath1.toString())
                        .option(CoreOptions.SCAN_SNAPSHOT_ID.key(), 1)
                        .load();
        List<Row> results = dataset.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1], [5,6,3]]");

        dataset = spark.read().format("paimon").option("path", tablePath1.toString()).load();
        results = dataset.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1], [5,6,3], [7,2,7], [8,4,8]]");
    }

    @Test
    public void testTravelToTimestamp() throws Exception {
        long anchor = System.currentTimeMillis();

        // snapshot 2
        writeTable(
                "t1",
                GenericRow.of(7, 2L, BinaryString.fromString("7")),
                GenericRow.of(8, 4L, BinaryString.fromString("8")));

        Dataset<Row> dataset =
                spark.read()
                        .format("paimon")
                        .option(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), anchor)
                        .load(tablePath1.toString());
        List<Row> results = dataset.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1], [5,6,3]]");

        dataset = spark.read().format("paimon").load(tablePath1.toString());
        results = dataset.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1], [5,6,3], [7,2,7], [8,4,8]]");
    }

    @Test
    public void testTravelToOldSchema() throws Exception {
        long anchor = System.currentTimeMillis();

        // new schema
        spark.sql("ALTER TABLE t1 ADD COLUMN dt STRING");

        // snapshot 2
        writeTable(
                "t1",
                GenericRow.of(7, 2L, BinaryString.fromString("7"), BinaryString.fromString("7")),
                GenericRow.of(8, 4L, BinaryString.fromString("8"), BinaryString.fromString("8")));

        Dataset<Row> dataset =
                spark.read()
                        .format("paimon")
                        .option("path", tablePath1.toString())
                        .option(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), anchor)
                        .load();
        List<Row> results = dataset.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1], [5,6,3]]");

        dataset = spark.read().format("paimon").option("path", tablePath1.toString()).load();
        results = dataset.collectAsList();
        assertThat(results.toString())
                .isEqualTo("[[1,2,1,null], [5,6,3,null], [7,2,7,7], [8,4,8,8]]");
    }

    @Test
    public void testTravelToNonExistedVersion() {
        assertThatThrownBy(
                        () ->
                                spark.read()
                                        .format("paimon")
                                        .option("path", tablePath1.toString())
                                        .option(CoreOptions.SCAN_SNAPSHOT_ID.key(), 3)
                                        .load()
                                        .collectAsList())
                .satisfies(
                        anyCauseMatches(
                                RuntimeException.class, "Fails to read snapshot from path file"));
    }

    @Test
    public void testTravelToNonExistedTimestamp() {
        Dataset<Row> dataset =
                spark.read()
                        .format("paimon")
                        .option("path", tablePath1.toString())
                        .option(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), 0)
                        .load();
        assertThat(dataset.collectAsList()).isEmpty();
    }

    @Test
    public void testTravelToTag() throws Exception {
        // snapshot 2
        writeTable(
                "t1",
                GenericRow.of(7, 2L, BinaryString.fromString("7")),
                GenericRow.of(8, 4L, BinaryString.fromString("8")));

        getTable("t1").createTag("tag1", 1);

        // read tag 'tag1'
        Dataset<Row> dataset =
                spark.read()
                        .format("paimon")
                        .option(CoreOptions.SCAN_TAG_NAME.key(), "tag1")
                        .load(tablePath1.toString());
        List<Row> results = dataset.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1], [5,6,3]]");

        // read latest
        dataset = spark.read().format("paimon").load(tablePath1.toString());
        results = dataset.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1], [5,6,3], [7,2,7], [8,4,8]]");
    }

    @Test
    public void testIllegalVersion() {
        assertThatThrownBy(
                        () ->
                                spark.read()
                                        .format("paimon")
                                        .option("path", tablePath1.toString())
                                        .option(CoreOptions.SCAN_SNAPSHOT_ID.key(), 1.5)
                                        .load()
                                        .collectAsList())
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Could not parse value '1.5' for key 'scan.snapshot-id'"));
    }
}
