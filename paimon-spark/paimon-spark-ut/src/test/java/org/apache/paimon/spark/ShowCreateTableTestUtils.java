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

import org.apache.spark.sql.SparkSession;

/** Helpers for asserting on {@code SHOW CREATE TABLE} output across Spark versions. */
public class ShowCreateTableTestUtils {

    private static final String EXPLICIT_BINARY_COLLATION = " COLLATE UTF8_BINARY";

    /** Runs {@code SHOW CREATE TABLE} and returns its output via {@link #stripBinaryCollation}. */
    public static String showCreateTable(SparkSession spark, String table) {
        return stripBinaryCollation(
                spark.sql("SHOW CREATE TABLE " + table).collectAsList().toString());
    }

    /**
     * Drops the explicit {@code COLLATE UTF8_BINARY} markers Spark 4.2 adds to every string-ish
     * column of {@code SHOW CREATE TABLE}.
     *
     * <p>SPARK-55372 made the command print the collation even for a column that has none, so that
     * replaying the emitted DDL cannot silently pick up a table- or schema-level {@code DEFAULT
     * COLLATION} instead. Paimon's string columns map to the {@code StringType} case object, which
     * Spark reads as "no explicit collation" and so renders this way; a built-in source such as
     * parquet gets the same treatment, so there is nothing Paimon-specific to fix. STRING, VARCHAR,
     * CHAR and the string leaves of ARRAY / MAP / STRUCT are all affected. The tests in this module
     * run against every supported Spark version from one copy of the source, so they assert on the
     * output with the marker removed rather than branching on the version.
     *
     * <p>Only the binary collation is stripped. A column carrying a real collation, say {@code
     * STRING COLLATE UTF8_LCASE}, still shows up, so an assertion cannot be fooled into accepting
     * the wrong collation.
     */
    public static String stripBinaryCollation(String showCreateTableOutput) {
        return showCreateTableOutput.replace(EXPLICIT_BINARY_COLLATION, "");
    }
}
