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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultValueAssignerTest {
    @TempDir java.nio.file.Path tempDir;

    private TableSchema tableSchema;

    @BeforeEach
    public void beforeEach() throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        Map<String, String> options = new HashMap<>();
        options.put(
                String.format(
                        "%s.%s.%s",
                        CoreOptions.FIELDS_PREFIX, "col4", CoreOptions.DEFAULT_VALUE_SUFFIX),
                "0");
        options.put(
                String.format(
                        "%s.%s.%s",
                        CoreOptions.FIELDS_PREFIX, "col5", CoreOptions.DEFAULT_VALUE_SUFFIX),
                "1");
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "col0", DataTypes.STRING()),
                                new DataField(1, "col1", DataTypes.STRING()),
                                new DataField(2, "col2", DataTypes.STRING()),
                                new DataField(3, "col3", DataTypes.STRING()),
                                new DataField(4, "col4", DataTypes.STRING()),
                                new DataField(5, "col5", DataTypes.STRING())),
                        Lists.newArrayList("col0"),
                        Collections.emptyList(),
                        options,
                        "");
        tableSchema = schemaManager.createTable(schema);
    }

    @Test
    public void testGeneralRow() {
        DefaultValueAssigner defaultValueAssigner = DefaultValueAssigner.create(tableSchema);
        RowType readRowType =
                tableSchema.projectedLogicalRowType(Lists.newArrayList("col5", "col4", "col0"));
        defaultValueAssigner = defaultValueAssigner.handleReadRowType(readRowType);
        InternalRow row = defaultValueAssigner.createDefaultValueRow().defaultValueRow();
        assertThat(String.format("%s|%s|%s", row.getString(0), row.getString(1), row.getString(2)))
                .isEqualTo("1|0|null");
    }

    @Test
    public void testHandlePredicate() {
        DefaultValueAssigner defaultValueAssigner = DefaultValueAssigner.create(tableSchema);
        PredicateBuilder predicateBuilder = new PredicateBuilder(tableSchema.logicalRowType());

        {
            Predicate predicate =
                    PredicateBuilder.and(
                            predicateBuilder.equal(predicateBuilder.indexOf("col5"), "100"),
                            predicateBuilder.equal(predicateBuilder.indexOf("col1"), "1"));
            Predicate actual = defaultValueAssigner.handlePredicate(predicate);
            assertThat(actual)
                    .isEqualTo(predicateBuilder.equal(predicateBuilder.indexOf("col1"), "1"));
        }

        {
            Predicate predicate =
                    PredicateBuilder.and(
                            predicateBuilder.equal(predicateBuilder.indexOf("col5"), "100"),
                            predicateBuilder.equal(predicateBuilder.indexOf("col4"), "1"));
            Predicate actual = defaultValueAssigner.handlePredicate(predicate);
            Assertions.assertThat(actual).isNull();
        }

        {
            Predicate actual = defaultValueAssigner.handlePredicate(null);
            Assertions.assertThat(actual).isNull();
        }

        {
            Predicate actual =
                    defaultValueAssigner.handlePredicate(
                            predicateBuilder.equal(predicateBuilder.indexOf("col1"), "1"));
            assertThat(actual)
                    .isEqualTo(predicateBuilder.equal(predicateBuilder.indexOf("col1"), "1"));
        }
    }
}
