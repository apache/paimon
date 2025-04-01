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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergDataFileMetaTest {

    @Test
    @DisplayName("Test partition is a required field")
    void testPartitionIsNotNull() {
        RowType partitionType = TestKeyValueGenerator.SINGLE_PARTITIONED_PART_TYPE;
        RowType schema = IcebergDataFileMeta.schema(partitionType);
        List<DataField> fields = schema.getFields();

        Optional<DataField> partitionField = fields.stream().filter(f -> f.id() == 102).findFirst();

        assertThat(partitionField).isPresent();
        assertThat(partitionField.get().name()).isEqualTo("partition");
        assertThat(partitionField.get().type()).isEqualTo(partitionType.notNull());
    }
}
