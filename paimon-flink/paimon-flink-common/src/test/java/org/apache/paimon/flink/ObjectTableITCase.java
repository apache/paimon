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

package org.apache.paimon.flink;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for object table. */
public class ObjectTableITCase extends CatalogITCaseBase {

    @Test
    public void testIllegalObjectTable() {
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE T (a INT, b INT, c INT) WITH ('type' = 'object-table')"))
                .rootCause()
                .hasMessageContaining("Schema of Object Table can be empty or");
        assertThatThrownBy(() -> sql("CREATE TABLE T WITH ('type' = 'object-table')"))
                .rootCause()
                .hasMessageContaining("Object table should have object-location option.");
    }

    @Test
    public void testObjectTableRefresh() throws IOException {
        Path objectLocation = new Path(path + "/object-location");
        FileIO fileIO = LocalFileIO.create();
        sql(
                "CREATE TABLE T WITH ('type' = 'object-table', 'object-location' = '%s')",
                objectLocation);

        // add new file
        fileIO.overwriteFileUtf8(new Path(objectLocation, "f0"), "1,2,3");
        sql("CALL sys.refresh_object_table('default.T')");
        assertThat(sql("SELECT name, length FROM T")).containsExactlyInAnyOrder(Row.of("f0", 5L));

        // add new file
        fileIO.overwriteFileUtf8(new Path(objectLocation, "f1"), "4,5,6");
        sql("CALL sys.refresh_object_table('default.T')");
        assertThat(sql("SELECT name, length FROM T"))
                .containsExactlyInAnyOrder(Row.of("f0", 5L), Row.of("f1", 5L));

        // delete file
        fileIO.deleteQuietly(new Path(objectLocation, "f0"));
        sql("CALL sys.refresh_object_table('default.T')");
        assertThat(sql("SELECT name, length FROM T")).containsExactlyInAnyOrder(Row.of("f1", 5L));
    }
}
