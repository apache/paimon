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

package org.apache.paimon.table.source;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.UpperTransform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests query-authorization projection expansion in {@link AbstractDataTableRead}. */
class AbstractDataTableReadTest {

    @Test
    void testMaskDependenciesPreserveNestedProjectionAndSkipUnselectedMasks() throws IOException {
        RowType fullProfile =
                new RowType(
                        Arrays.asList(
                                new DataField(1, "a", DataTypes.INT()),
                                new DataField(2, "b", DataTypes.STRING())));
        DataField profile = new DataField(0, "profile", fullProfile);
        DataField protectedField = new DataField(3, "protected", DataTypes.STRING());
        DataField seed = new DataField(4, "seed", DataTypes.STRING());
        DataField unused = new DataField(5, "unused", DataTypes.STRING());
        DataField unusedSeed = new DataField(6, "unused_seed", DataTypes.STRING());
        TableSchema schema =
                new TableSchema(
                        1,
                        Arrays.asList(profile, protectedField, seed, unused, unusedSeed),
                        6,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null);

        RowType prunedProfile =
                new RowType(Collections.singletonList(new DataField(2, "b", DataTypes.STRING())));
        RowType requestedType =
                new RowType(Arrays.asList(profile.newType(prunedProfile), protectedField));
        TestingDataTableRead read = new TestingDataTableRead(schema);
        read.withReadType(requestedType);

        Map<String, String> masks = new LinkedHashMap<>();
        masks.put(
                "protected",
                JsonSerdeUtil.toFlatJson(
                        new UpperTransform(
                                Collections.singletonList(
                                        new FieldRef(4, "seed", DataTypes.STRING())))));
        masks.put(
                "unused",
                JsonSerdeUtil.toFlatJson(
                        new UpperTransform(
                                Collections.singletonList(
                                        new FieldRef(6, "unused_seed", DataTypes.STRING())))));

        read.createAuthedReader(new TableQueryAuthResult(null, masks));

        assertThat(read.appliedReadType().getFieldNames())
                .containsExactly("profile", "protected", "seed");
        assertThat(read.appliedReadType().getTypeAt(0)).isEqualTo(prunedProfile);

        read.createAuthedReader(new TableQueryAuthResult(null, Collections.emptyMap()));

        assertThat(read.appliedReadType()).isEqualTo(requestedType);
    }

    private static class TestingDataTableRead extends AbstractDataTableRead {

        private RowType appliedReadType;

        private TestingDataTableRead(TableSchema schema) {
            super(schema);
        }

        @Override
        public void applyReadType(RowType readType) {
            appliedReadType = readType;
        }

        @Override
        public RecordReader<InternalRow> reader(Split split) {
            return new RecordReader<InternalRow>() {
                @Override
                public RecordIterator<InternalRow> readBatch() {
                    return null;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        protected InnerTableRead innerWithFilter(Predicate predicate) {
            return this;
        }

        private void createAuthedReader(TableQueryAuthResult authResult) throws IOException {
            createDataReader(mock(Split.class), authResult);
        }

        private RowType appliedReadType() {
            return appliedReadType;
        }
    }
}
