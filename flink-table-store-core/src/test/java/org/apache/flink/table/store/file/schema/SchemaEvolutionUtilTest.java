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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.table.api.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SchemaEvolutionUtil}. */
public class SchemaEvolutionUtilTest {
    @Test
    public void testCreateIndexMapping() {
        List<DataField> dataFields =
                Arrays.asList(
                        new DataField(0, "a", new AtomicDataType(DataTypes.INT().getLogicalType())),
                        new DataField(1, "b", new AtomicDataType(DataTypes.INT().getLogicalType())),
                        new DataField(2, "c", new AtomicDataType(DataTypes.INT().getLogicalType())),
                        new DataField(
                                3, "d", new AtomicDataType(DataTypes.INT().getLogicalType())));
        List<DataField> tableFields =
                Arrays.asList(
                        new DataField(1, "c", new AtomicDataType(DataTypes.INT().getLogicalType())),
                        new DataField(3, "a", new AtomicDataType(DataTypes.INT().getLogicalType())),
                        new DataField(5, "d", new AtomicDataType(DataTypes.INT().getLogicalType())),
                        new DataField(
                                6, "e", new AtomicDataType(DataTypes.INT().getLogicalType())));
        int[] indexMapping = SchemaEvolutionUtil.createIndexMapping(tableFields, dataFields);

        assertThat(indexMapping.length).isEqualTo(tableFields.size()).isEqualTo(4);
        assertThat(indexMapping[0]).isEqualTo(1);
        assertThat(indexMapping[1]).isEqualTo(3);
        assertThat(indexMapping[2]).isLessThan(0);
        assertThat(indexMapping[3]).isLessThan(0);
    }
}
