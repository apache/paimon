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

package org.apache.paimon.flink.procedure;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.extraction.TypeInferenceExtractor;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CloneProcedure}. */
public class CloneProcedureTest {

    @Test
    public void testNamedOptionalArgumentsArePreserved() {
        TypeInference inference =
                TypeInferenceExtractor.forProcedure(
                        new TestingDataTypeFactory(), CloneProcedure.class);

        assertThat(inference.getNamedArguments())
                .hasValue(
                        Arrays.asList(
                                "database",
                                "table",
                                "catalog_conf",
                                "target_database",
                                "target_table",
                                "target_catalog_conf",
                                "parallelism",
                                "where",
                                "included_tables",
                                "excluded_tables",
                                "prefer_file_format",
                                "clone_from",
                                "meta_only",
                                "clone_if_exists",
                                "clone_mode",
                                "path_mapping"));
        assertThat(inference.getOptionalArguments())
                .hasValue(Collections.nCopies(16, Boolean.TRUE));
    }

    private static class TestingDataTypeFactory implements DataTypeFactory {

        @Override
        public DataType createDataType(AbstractDataType<?> abstractDataType) {
            if (abstractDataType instanceof DataType) {
                return (DataType) abstractDataType;
            }
            return ((UnresolvedDataType) abstractDataType).toDataType(this);
        }

        @Override
        public DataType createDataType(String typeString) {
            switch (typeString) {
                case "STRING":
                    return DataTypes.STRING();
                case "INT":
                    return DataTypes.INT();
                case "BOOLEAN":
                    return DataTypes.BOOLEAN();
                default:
                    throw new UnsupportedOperationException(typeString);
            }
        }

        @Override
        public DataType createDataType(UnresolvedIdentifier identifier) {
            throw new UnsupportedOperationException(identifier.toString());
        }

        @Override
        public <T> DataType createDataType(Class<T> clazz) {
            if (clazz == String[].class) {
                return DataTypes.ARRAY(DataTypes.STRING()).bridgedTo(clazz);
            }
            throw new UnsupportedOperationException(clazz.getName());
        }

        @Override
        public <T> DataType createDataType(TypeInformation<T> typeInformation) {
            throw new UnsupportedOperationException(typeInformation.toString());
        }

        @Override
        public <T> DataType createRawDataType(Class<T> clazz) {
            throw new UnsupportedOperationException(clazz.getName());
        }

        @Override
        public <T> DataType createRawDataType(TypeInformation<T> typeInformation) {
            throw new UnsupportedOperationException(typeInformation.toString());
        }

        @Override
        public LogicalType createLogicalType(String typeString) {
            return createDataType(typeString).getLogicalType();
        }

        @Override
        public LogicalType createLogicalType(UnresolvedIdentifier identifier) {
            throw new UnsupportedOperationException(identifier.toString());
        }
    }
}
