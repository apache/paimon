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

package org.apache.paimon.eslib.index;

import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.VectorType;

import org.elasticsearch.eslib.api.model.BuiltinAnalyzer;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.ScalarFieldType;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses Paimon Options into ESLib FieldIndexConfig for each field.
 *
 * <p>Options format (in table properties):
 *
 * <pre>
 *   global-index.es-index.fields.vector_field.algorithm = diskbbq
 *   global-index.es-index.fields.vector_field.dimension = 128
 *   global-index.es-index.fields.text_field.analyzer = standard
 *   global-index.es-index.fields.id_field.type = keyword
 * </pre>
 */
public class ESIndexOptions {

    private static final String FIELDS_PREFIX = "fields.";

    private final Map<String, FieldIndexConfig> fieldConfigs;

    public ESIndexOptions(List<DataField> fields, Options options) {
        this.fieldConfigs = new LinkedHashMap<>();
        for (DataField field : fields) {
            fieldConfigs.put(field.name(), parseFieldConfig(field, options));
        }
    }

    public Map<String, FieldIndexConfig> getFieldConfigs() {
        return fieldConfigs;
    }

    public FieldIndexConfig getConfig(String fieldName) {
        return fieldConfigs.get(fieldName);
    }

    private FieldIndexConfig parseFieldConfig(DataField field, Options options) {
        String prefix = FIELDS_PREFIX + field.name() + ".";
        DataType dataType = field.type();

        // Explicit type override takes priority
        String explicitType = options.getString(prefix + "type", null);
        if (explicitType != null) {
            return parseExplicitType(field.name(), explicitType, dataType, options, prefix);
        }

        if (isVectorType(dataType)) {
            return parseVectorConfig(field.name(), dataType, options, prefix);
        } else if (isTextType(dataType)) {
            // String fields: check if analyzer is set → FULLTEXT; otherwise → KEYWORD
            String analyzer = options.getString(prefix + "analyzer", null);
            if (analyzer != null) {
                return FieldIndexConfig.builder(field.name(), FieldIndexConfig.IndexType.FULLTEXT)
                        .analyzer(BuiltinAnalyzer.fromName(analyzer))
                        .build();
            }
            return FieldIndexConfig.builder(field.name(), FieldIndexConfig.IndexType.KEYWORD)
                    .scalarType(ScalarFieldType.KEYWORD)
                    .build();
        } else if (isTimestampType(dataType)) {
            return FieldIndexConfig.builder(field.name(), FieldIndexConfig.IndexType.DATE)
                    .scalarType(ScalarFieldType.DATE)
                    .build();
        } else {
            return FieldIndexConfig.builder(field.name(), FieldIndexConfig.IndexType.SCALAR)
                    .scalarType(mapScalarType(dataType))
                    .build();
        }
    }

    private FieldIndexConfig parseExplicitType(
            String fieldName, String typeName, DataType dataType, Options options, String prefix) {
        switch (typeName.toLowerCase()) {
            case "fulltext":
                String analyzer = options.getString(prefix + "analyzer", "standard");
                return FieldIndexConfig.builder(fieldName, FieldIndexConfig.IndexType.FULLTEXT)
                        .analyzer(BuiltinAnalyzer.fromName(analyzer))
                        .build();
            case "keyword":
                return FieldIndexConfig.builder(fieldName, FieldIndexConfig.IndexType.KEYWORD)
                        .scalarType(ScalarFieldType.KEYWORD)
                        .build();
            case "geo_point":
                return FieldIndexConfig.builder(fieldName, FieldIndexConfig.IndexType.GEO_POINT)
                        .scalarType(ScalarFieldType.GEO_POINT)
                        .build();
            case "date":
                return FieldIndexConfig.builder(fieldName, FieldIndexConfig.IndexType.DATE)
                        .scalarType(ScalarFieldType.DATE)
                        .build();
            case "vector":
                return parseVectorConfig(fieldName, dataType, options, prefix);
            default:
                return FieldIndexConfig.builder(fieldName, FieldIndexConfig.IndexType.SCALAR)
                        .scalarType(mapScalarType(dataType))
                        .build();
        }
    }

    private FieldIndexConfig parseVectorConfig(
            String fieldName, DataType dataType, Options options, String prefix) {
        String algorithm = options.getString(prefix + "algorithm", "hnsw");
        int dimension = options.getInteger(prefix + "dimension", inferDimension(dataType));
        String metric = options.getString(prefix + "metric", "cosine");

        Map<String, String> params = new LinkedHashMap<>();
        String mStr = options.getString(prefix + "m", null);
        if (mStr != null) {
            params.put("m", mStr);
        }
        String efStr = options.getString(prefix + "ef_construction", null);
        if (efStr != null) {
            params.put("ef_construction", efStr);
        }
        String vpcStr = options.getString(prefix + "vectors_per_cluster", null);
        if (vpcStr != null) {
            params.put("vectors_per_cluster", vpcStr);
        }

        return FieldIndexConfig.builder(fieldName, FieldIndexConfig.IndexType.VECTOR)
                .algorithm(VectorAlgorithm.fromName(algorithm))
                .dimension(dimension)
                .metric(metric)
                .algorithmParams(params)
                .build();
    }

    private static ScalarFieldType mapScalarType(DataType type) {
        if (type instanceof ArrayType) {
            return mapArrayScalarType((ArrayType) type);
        }
        switch (type.getTypeRoot()) {
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return ScalarFieldType.INT;
            case BIGINT:
                return ScalarFieldType.LONG;
            case FLOAT:
                return ScalarFieldType.FLOAT;
            case DOUBLE:
                return ScalarFieldType.DOUBLE;
            default:
                return ScalarFieldType.KEYWORD;
        }
    }

    private static ScalarFieldType mapArrayScalarType(ArrayType type) {
        DataType elementType = type.getElementType();
        switch (elementType.getTypeRoot()) {
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return ScalarFieldType.INT;
            case BIGINT:
                return ScalarFieldType.LONG;
            case CHAR:
            case VARCHAR:
                return ScalarFieldType.KEYWORD;
            default:
                throw new IllegalArgumentException(
                        "Unsupported scalar array element type for es-index: " + elementType);
        }
    }

    private static boolean isVectorType(DataType type) {
        if (type instanceof VectorType) {
            return true;
        }
        if (type instanceof ArrayType) {
            DataType elementType = ((ArrayType) type).getElementType();
            return elementType.getTypeRoot() == DataTypeRoot.FLOAT;
        }
        return false;
    }

    private static boolean isTextType(DataType type) {
        return type.getTypeRoot() == DataTypeRoot.VARCHAR
                || type.getTypeRoot() == DataTypeRoot.CHAR;
    }

    private static boolean isTimestampType(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        return root == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || root == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                || root == DataTypeRoot.DATE;
    }

    private static int inferDimension(DataType type) {
        if (type instanceof VectorType) {
            return ((VectorType) type).getLength();
        }
        return 0;
    }
}
