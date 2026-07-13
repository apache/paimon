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

import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.elasticsearch.eslib.api.model.BuiltinAnalyzer;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.ScalarFieldType;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;
import org.elasticsearch.eslib.diskbbq.es94.ES940DiskBBQVectorsFormat;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Parses Paimon Options into ESLib FieldIndexConfig for each field.
 *
 * <p>Keys are read from the table options using the persisted {@code global-index.es-index.*}
 * convention (same keys the table schema stores and the ES mount reads). A field-level key {@code
 * global-index.es-index.fields.<field>.<key>} takes precedence over the index-type-level key {@code
 * global-index.es-index.<key>}.
 *
 * <pre>
 *   global-index.es-index.metric = cosine                       # index-type-level default
 *   global-index.es-index.fields.vector_field.algorithm = diskbbq   # field-level override
 *   global-index.es-index.fields.vector_field.dimension = 128
 *   global-index.es-index.fields.text_field.analyzer = standard
 *   global-index.es-index.fields.id_field.type = keyword
 * </pre>
 */
public class ESIndexOptions {

    private static final int MAX_VECTOR_DIMENSION = 4096;

    /**
     * Index-type-level key prefix (e.g. {@code global-index.es-index.metric}); overridden by
     * field-level keys. Aligned with the persisted table-property / ES-mount convention ({@code
     * global-index.<indexType>.*}) so build, read and mount all read the same keys. The {@code
     * es-index} part still comes from {@link ESIndexGlobalIndexerFactory#IDENTIFIER}.
     */
    private static final String INDEX_TYPE_PREFIX =
            "global-index." + ESIndexGlobalIndexerFactory.IDENTIFIER + ".";

    /** Field-level key prefix, e.g. {@code global-index.es-index.fields.<field>.algorithm}. */
    private static final String FIELDS_PREFIX = INDEX_TYPE_PREFIX + "fields.";

    /** Suffix of the keyword multi-field sub-field of a FULLTEXT column. */
    public static final String KEYWORD_SUBFIELD_SUFFIX = ".keyword";

    /** Suffix of the full-text multi-field sub-field of a KEYWORD column. */
    public static final String FULLTEXT_SUBFIELD_SUFFIX = ".fulltext";

    /** Internal field used to distinguish a non-null empty array from a null array. */
    static final String ARRAY_PRESENCE_SUBFIELD_SUFFIX = ".__paimon_array_present";

    private final Map<String, FieldIndexConfig> fieldConfigs;

    public ESIndexOptions(List<DataField> fields, Options options) {
        this.fieldConfigs = new LinkedHashMap<>();
        Set<String> logicalFieldNames = new HashSet<>();
        for (DataField field : fields) {
            if (!logicalFieldNames.add(field.name())) {
                throw new IllegalArgumentException(
                        "Duplicate field name in es-index: " + field.name());
            }
        }
        for (DataField field : fields) {
            FieldIndexConfig config = parseFieldConfig(field, options);
            fieldConfigs.put(field.name(), config);
            // Text fields always support both analyzed search and exact matching. The configured
            // type selects the primary field; the complementary capability is stored in a
            // multi-field sub-field.
            if (isTextType(field.type())
                    && config.indexType() == FieldIndexConfig.IndexType.FULLTEXT) {
                String subField = field.name() + KEYWORD_SUBFIELD_SUFFIX;
                putGeneratedFieldConfig(
                        logicalFieldNames,
                        subField,
                        FieldIndexConfig.builder(subField, FieldIndexConfig.IndexType.KEYWORD)
                                .scalarType(ScalarFieldType.KEYWORD)
                                .build());
            } else if (isTextType(field.type())
                    && config.indexType() == FieldIndexConfig.IndexType.KEYWORD) {
                String subField = field.name() + FULLTEXT_SUBFIELD_SUFFIX;
                String analyzer = resolve(options, field.name(), "analyzer", "standard");
                putGeneratedFieldConfig(
                        logicalFieldNames,
                        subField,
                        FieldIndexConfig.builder(subField, FieldIndexConfig.IndexType.FULLTEXT)
                                .analyzer(parseAnalyzer(field.name(), analyzer))
                                .build());
            }
            if (field.type() instanceof ArrayType) {
                String presenceField = field.name() + ARRAY_PRESENCE_SUBFIELD_SUFFIX;
                putGeneratedFieldConfig(
                        logicalFieldNames,
                        presenceField,
                        FieldIndexConfig.builder(presenceField, FieldIndexConfig.IndexType.SCALAR)
                                .scalarType(ScalarFieldType.INT)
                                .build());
            }
        }
    }

    private void putGeneratedFieldConfig(
            Set<String> logicalFieldNames,
            String generatedFieldName,
            FieldIndexConfig generatedConfig) {
        if (logicalFieldNames.contains(generatedFieldName)
                || fieldConfigs.putIfAbsent(generatedFieldName, generatedConfig) != null) {
            throw new IllegalArgumentException(
                    "Field name conflicts with reserved es-index generated field: "
                            + generatedFieldName);
        }
    }

    private ESIndexOptions(Map<String, FieldIndexConfig> fieldConfigs) {
        this.fieldConfigs = new LinkedHashMap<>(fieldConfigs);
    }

    /** Recreates the exact build-time configuration persisted in the index metadata. */
    static ESIndexOptions fromFieldConfigs(Map<String, FieldIndexConfig> fieldConfigs) {
        return new ESIndexOptions(fieldConfigs);
    }

    public Map<String, FieldIndexConfig> getFieldConfigs() {
        return Collections.unmodifiableMap(fieldConfigs);
    }

    public FieldIndexConfig getConfig(String fieldName) {
        return fieldConfigs.get(fieldName);
    }

    /** Returns the keyword multi-field sub-field name for {@code fieldName} if one exists. */
    public String keywordSubField(String fieldName) {
        String subField = fieldName + KEYWORD_SUBFIELD_SUFFIX;
        return fieldConfigs.containsKey(subField) ? subField : null;
    }

    /** Returns the full-text multi-field sub-field name for {@code fieldName} if one exists. */
    public String fullTextSubField(String fieldName) {
        String subField = fieldName + FULLTEXT_SUBFIELD_SUFFIX;
        return fieldConfigs.containsKey(subField) ? subField : null;
    }

    /** Returns the internal presence field for an array, or {@code null} for legacy indexes. */
    public String arrayPresenceField(String fieldName) {
        String subField = fieldName + ARRAY_PRESENCE_SUBFIELD_SUFFIX;
        return fieldConfigs.containsKey(subField) ? subField : null;
    }

    /** Returns the physical FULLTEXT field used to search the logical Paimon field. */
    public String fullTextSearchField(String fieldName) {
        FieldIndexConfig config = fieldConfigs.get(fieldName);
        if (config != null && config.indexType() == FieldIndexConfig.IndexType.FULLTEXT) {
            return fieldName;
        }
        return fullTextSubField(fieldName);
    }

    /**
     * Resolves a config value for {@code fieldName}/{@code key}: the field-level key {@code
     * fields.<field>.<key>} takes precedence over the index-type-level key {@code es-index.<key>};
     * returns {@code defaultValue} when neither is set.
     */
    private static String resolve(
            Options options, String fieldName, String key, String defaultValue) {
        String value = resolveField(options, fieldName, key);
        if (value == null) {
            value = options.getString(INDEX_TYPE_PREFIX + key, null);
        }
        return value == null ? defaultValue : value;
    }

    private static String resolveField(Options options, String fieldName, String key) {
        return options.getString(FIELDS_PREFIX + fieldName + "." + key, null);
    }

    private FieldIndexConfig parseFieldConfig(DataField field, Options options) {
        DataType dataType = field.type();

        // Explicit type override takes priority
        String explicitType = resolve(options, field.name(), "type", null);
        if (explicitType != null) {
            return parseExplicitType(field.name(), explicitType, dataType, options);
        }

        if (isVectorType(dataType)) {
            return parseVectorConfig(field.name(), dataType, options);
        } else if (isTextType(dataType)) {
            // String fields default to FULLTEXT (analyzed with the standard analyzer) so full-text
            // search always works on a text column; a keyword sub-field (<field>.keyword) is added
            // below for exact filters — the ES text/keyword multi-field, so both capabilities are
            // available. An explicit analyzer overrides the default. Configuring type=keyword
            // makes the exact field primary and adds a FULLTEXT sub-field instead.
            String analyzer = resolve(options, field.name(), "analyzer", "standard");
            return FieldIndexConfig.builder(field.name(), FieldIndexConfig.IndexType.FULLTEXT)
                    .analyzer(parseAnalyzer(field.name(), analyzer))
                    .build();
        } else if (isTemporalType(dataType)) {
            // ESLib does not implement DATE scalar filters. Store DATE/TIMESTAMP as a
            // long scalar instead (DATE = epoch day, TIMESTAMP = epoch millis) so predicate
            // filtering uses the supported LONG query path.
            return FieldIndexConfig.builder(field.name(), FieldIndexConfig.IndexType.SCALAR)
                    .scalarType(ScalarFieldType.LONG)
                    .build();
        } else {
            return FieldIndexConfig.builder(field.name(), FieldIndexConfig.IndexType.SCALAR)
                    .scalarType(mapScalarType(dataType))
                    .build();
        }
    }

    private FieldIndexConfig parseExplicitType(
            String fieldName, String typeName, DataType dataType, Options options) {
        switch (typeName.toLowerCase(Locale.ROOT)) {
            case "fulltext":
                requireTextType(fieldName, typeName, dataType);
                String analyzer = resolve(options, fieldName, "analyzer", "standard");
                return FieldIndexConfig.builder(fieldName, FieldIndexConfig.IndexType.FULLTEXT)
                        .analyzer(parseAnalyzer(fieldName, analyzer))
                        .build();
            case "keyword":
                requireTextType(fieldName, typeName, dataType);
                return FieldIndexConfig.builder(fieldName, FieldIndexConfig.IndexType.KEYWORD)
                        .scalarType(ScalarFieldType.KEYWORD)
                        .build();
            case "geo_point":
                throw new IllegalArgumentException(
                        "Explicit es-index type 'geo_point' is not supported for field '"
                                + fieldName
                                + "'.");
            case "date":
                if (!isTemporalType(dataType)) {
                    throw incompatibleType(fieldName, typeName, dataType);
                }
                return FieldIndexConfig.builder(fieldName, FieldIndexConfig.IndexType.SCALAR)
                        .scalarType(ScalarFieldType.LONG)
                        .build();
            case "vector":
                if (!isVectorType(dataType)) {
                    throw incompatibleType(fieldName, typeName, dataType);
                }
                return parseVectorConfig(fieldName, dataType, options);
            default:
                throw new IllegalArgumentException(
                        "Unknown es-index type '"
                                + typeName
                                + "' for field '"
                                + fieldName
                                + "'. Supported values: vector, fulltext, keyword, date.");
        }
    }

    private FieldIndexConfig parseVectorConfig(
            String fieldName, DataType dataType, Options options) {
        String algorithm = resolve(options, fieldName, "algorithm", "hnsw");
        VectorAlgorithm vectorAlgorithm = VectorAlgorithm.fromName(algorithm);
        if (vectorAlgorithm == VectorAlgorithm.NATIVE) {
            throw new IllegalArgumentException(
                    "Vector algorithm 'native' is not supported by paimon-eslib; "
                            + "use 'hnsw' or 'diskbbq'");
        }

        String dimStr = resolve(options, fieldName, "dimension", null);
        int inferredDimension = inferDimension(dataType);
        int dimension;
        if (dimStr == null) {
            if (inferredDimension <= 0) {
                throw new IllegalArgumentException(
                        "Vector field '"
                                + fieldName
                                + "' requires a positive dimension; ARRAY<FLOAT> has no fixed "
                                + "dimension in its data type.");
            }
            dimension = inferredDimension;
        } else {
            dimension = parsePositiveInteger(fieldName, "dimension", dimStr);
            if (inferredDimension > 0 && dimension != inferredDimension) {
                throw new IllegalArgumentException(
                        "Vector field '"
                                + fieldName
                                + "' dimension "
                                + dimension
                                + " does not match the VECTOR type dimension "
                                + inferredDimension
                                + ".");
            }
        }
        if (dimension > MAX_VECTOR_DIMENSION) {
            throw new IllegalArgumentException(
                    "Vector field '"
                            + fieldName
                            + "' dimension "
                            + dimension
                            + " exceeds the maximum supported dimension "
                            + MAX_VECTOR_DIMENSION
                            + ".");
        }
        String metric = validateMetric(fieldName, resolve(options, fieldName, "metric", "cosine"));

        Map<String, String> params = new LinkedHashMap<>();
        // Table-level algorithm parameters are defaults only for fields using that algorithm. A
        // table can contain HNSW and DiskBBQ fields at the same time, so a global HNSW parameter
        // must not make a DiskBBQ field invalid (and vice versa). Field-level parameters remain
        // strict because they unambiguously target this field.
        String mStr =
                vectorAlgorithm == VectorAlgorithm.HNSW
                        ? resolve(options, fieldName, "m", null)
                        : resolveField(options, fieldName, "m");
        String efStr =
                vectorAlgorithm == VectorAlgorithm.HNSW
                        ? resolve(options, fieldName, "ef_construction", null)
                        : resolveField(options, fieldName, "ef_construction");
        if (vectorAlgorithm == VectorAlgorithm.HNSW) {
            putBoundedPositiveIntegerParameter(
                    params, fieldName, "m", mStr, Lucene99HnswVectorsFormat.MAXIMUM_MAX_CONN);
            putBoundedPositiveIntegerParameter(
                    params,
                    fieldName,
                    "ef_construction",
                    efStr,
                    Lucene99HnswVectorsFormat.MAXIMUM_BEAM_WIDTH);
        } else if (mStr != null || efStr != null) {
            throw new IllegalArgumentException(
                    "Vector field '"
                            + fieldName
                            + "' configures m/ef_construction but algorithm is "
                            + algorithm
                            + "; these parameters require algorithm=hnsw.");
        }
        String vpcStr =
                vectorAlgorithm == VectorAlgorithm.DISKBBQ
                        ? resolve(options, fieldName, "vectors_per_cluster", null)
                        : resolveField(options, fieldName, "vectors_per_cluster");
        if (vectorAlgorithm == VectorAlgorithm.DISKBBQ) {
            putBoundedPositiveIntegerParameter(
                    params, fieldName, "vectors_per_cluster", vpcStr, 64, 1 << 16);
        } else if (vpcStr != null) {
            throw new IllegalArgumentException(
                    "Vector field '"
                            + fieldName
                            + "' configures vectors_per_cluster but algorithm is "
                            + algorithm
                            + "; this parameter requires algorithm=diskbbq.");
        }
        String cpcStr =
                vectorAlgorithm == VectorAlgorithm.DISKBBQ
                        ? resolve(options, fieldName, "centroids_per_parent_cluster", null)
                        : resolveField(options, fieldName, "centroids_per_parent_cluster");
        if (vectorAlgorithm == VectorAlgorithm.DISKBBQ) {
            putBoundedPositiveIntegerParameter(
                    params,
                    fieldName,
                    "centroids_per_parent_cluster",
                    cpcStr,
                    ES940DiskBBQVectorsFormat.MIN_CENTROIDS_PER_PARENT_CLUSTER,
                    ES940DiskBBQVectorsFormat.MAX_CENTROIDS_PER_PARENT_CLUSTER);
        } else if (cpcStr != null) {
            throw new IllegalArgumentException(
                    "Vector field '"
                            + fieldName
                            + "' configures centroids_per_parent_cluster but algorithm is "
                            + algorithm
                            + "; this parameter requires algorithm=diskbbq.");
        }

        return FieldIndexConfig.builder(fieldName, FieldIndexConfig.IndexType.VECTOR)
                .algorithm(vectorAlgorithm)
                .dimension(dimension)
                .metric(metric)
                .algorithmParams(params)
                .build();
    }

    private static BuiltinAnalyzer parseAnalyzer(String fieldName, String analyzerName) {
        BuiltinAnalyzer analyzer = BuiltinAnalyzer.fromName(analyzerName);
        if (analyzer == BuiltinAnalyzer.IK_SMART || analyzer == BuiltinAnalyzer.IK_MAX_WORD) {
            throw new IllegalArgumentException(
                    "Analyzer '"
                            + analyzerName
                            + "' is not supported by paimon-eslib for field '"
                            + fieldName
                            + "'; use standard, whitespace, simple, or keyword.");
        }
        return analyzer;
    }

    private static void putBoundedPositiveIntegerParameter(
            Map<String, String> params,
            String fieldName,
            String key,
            String rawValue,
            int maximum) {
        if (rawValue == null) {
            return;
        }
        final int value;
        try {
            value = Integer.parseInt(rawValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid HNSW parameter '"
                            + key
                            + "' for vector field '"
                            + fieldName
                            + "': must be an integer between 1 and "
                            + maximum
                            + ".",
                    e);
        }
        if (value <= 0 || value > maximum) {
            throw new IllegalArgumentException(
                    "Invalid HNSW parameter '"
                            + key
                            + "' for vector field '"
                            + fieldName
                            + "': must be an integer between 1 and "
                            + maximum
                            + ".");
        }
        params.put(key, String.valueOf(value));
    }

    private static void putBoundedPositiveIntegerParameter(
            Map<String, String> params,
            String fieldName,
            String key,
            String rawValue,
            int minimum,
            int maximum) {
        if (rawValue == null) {
            return;
        }
        final int value;
        try {
            value = Integer.parseInt(rawValue);
        } catch (NumberFormatException e) {
            throw invalidIntegerParameter(fieldName, key, minimum, maximum, e);
        }
        if (value < minimum || value > maximum) {
            throw invalidIntegerParameter(fieldName, key, minimum, maximum, null);
        }
        params.put(key, String.valueOf(value));
    }

    private static IllegalArgumentException invalidIntegerParameter(
            String fieldName, String key, int minimum, int maximum, Throwable cause) {
        String message =
                "Invalid parameter '"
                        + key
                        + "' for vector field '"
                        + fieldName
                        + "': must be an integer between "
                        + minimum
                        + " and "
                        + maximum
                        + ".";
        return cause == null
                ? new IllegalArgumentException(message)
                : new IllegalArgumentException(message, cause);
    }

    private static int parsePositiveInteger(String fieldName, String key, String rawValue) {
        final int value;
        try {
            value = Integer.parseInt(rawValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid " + key + " for vector field '" + fieldName + "': " + rawValue, e);
        }
        if (value <= 0) {
            throw new IllegalArgumentException(
                    "Invalid " + key + " for vector field '" + fieldName + "': must be positive.");
        }
        return value;
    }

    private static String validateMetric(String fieldName, String metric) {
        String normalized = metric.toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "cosine":
            case "l2":
            case "euclidean":
            case "dot_product":
            case "dp":
            case "inner_product":
            case "mip":
            case "maximum_inner_product":
                return normalized;
            default:
                throw new IllegalArgumentException(
                        "Unknown vector metric '"
                                + metric
                                + "' for field '"
                                + fieldName
                                + "'. Supported values: cosine, l2, euclidean, dot_product, "
                                + "inner_product, mip, maximum_inner_product.");
        }
    }

    private static void requireTextType(String fieldName, String typeName, DataType dataType) {
        if (!isTextType(dataType)) {
            throw incompatibleType(fieldName, typeName, dataType);
        }
    }

    private static IllegalArgumentException incompatibleType(
            String fieldName, String typeName, DataType dataType) {
        return new IllegalArgumentException(
                "Explicit es-index type '"
                        + typeName
                        + "' is incompatible with field '"
                        + fieldName
                        + "' of type "
                        + dataType
                        + ".");
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
            case CHAR:
            case VARCHAR:
                return ScalarFieldType.KEYWORD;
            default:
                // Fail fast on scalar roots the writer cannot extract (e.g. BOOLEAN, DECIMAL,
                // BINARY). Mapping them to KEYWORD would defer the failure to build time, where
                // ESIndexGlobalIndexWriter.extractScalar falls through to getString() and throws a
                // ClassCastException. Mirror mapArrayScalarType, which rejects unsupported types.
                throw new IllegalArgumentException("Unsupported scalar type for es-index: " + type);
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
            return ((VectorType) type).getElementType().getTypeRoot() == DataTypeRoot.FLOAT;
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

    private static boolean isTemporalType(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        return root == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || root == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                || root == DataTypeRoot.DATE
                || root == DataTypeRoot.TIME_WITHOUT_TIME_ZONE;
    }

    private static int inferDimension(DataType type) {
        if (type instanceof VectorType) {
            return ((VectorType) type).getLength();
        }
        return 0;
    }
}
