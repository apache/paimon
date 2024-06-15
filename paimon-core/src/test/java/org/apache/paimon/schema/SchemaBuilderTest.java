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

package org.apache.paimon.schema;

import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link Schema.Builder}. */
public class SchemaBuilderTest {

    @Test
    public void testDuplicateColumns() {
        Schema.Builder builder =
                Schema.newBuilder().column("id", DataTypes.INT()).column("id", DataTypes.INT());

        assertThatThrownBy(builder::build)
                .satisfies(
                        anyCauseMatches(
                                IllegalStateException.class,
                                "Table column [id, id] must not contain duplicate fields. Found: [id]"));
    }

    @Test
    public void testDuplicatePrimaryKeys() {
        Schema.Builder builder =
                Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id", "id");

        assertThatThrownBy(builder::build)
                .satisfies(
                        anyCauseMatches(
                                IllegalStateException.class,
                                "Primary key constraint [id, id] must not contain duplicate columns. Found: [id]"));
    }

    @Test
    public void testDuplicatePartitionKeys() {
        Schema.Builder builder =
                Schema.newBuilder().column("id", DataTypes.INT()).partitionKeys("id", "id");

        assertThatThrownBy(builder::build)
                .satisfies(
                        anyCauseMatches(
                                IllegalStateException.class,
                                "Partition key constraint [id, id] must not contain duplicate columns. Found: [id]"));
    }

    @Test
    public void testHighestFieldId() {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(
                                "col1",
                                DataTypes.ROW(
                                        DataTypes.STRING(),
                                        DataTypes.INT(),
                                        DataTypes.ARRAY(DataTypes.STRING())))
                        .column("col2", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .column("col3", DataTypes.ARRAY(DataTypes.ROW(DataTypes.STRING())))
                        .partitionKeys("id")
                        .partitionKeys("id");
        assertThat(builder.getHighestFieldId()).isEqualTo(7);
    }
}
