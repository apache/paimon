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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.FirstRowMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction;
import org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE;
import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
import static org.apache.paimon.CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE;
import static org.apache.paimon.CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP;
import static org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction.SEQUENCE_GROUP;
import static org.apache.paimon.table.SpecialFields.KEY_FIELD_ID_START;
import static org.apache.paimon.table.SpecialFields.KEY_FIELD_PREFIX;

/** Utils for creating changelog table with primary keys. */
public class PrimaryKeyTableUtils {

    public static RowType addKeyNamePrefix(RowType type) {
        return new RowType(addKeyNamePrefix(type.getFields()));
    }

    public static List<DataField> addKeyNamePrefix(List<DataField> keyFields) {
        return keyFields.stream()
                .map(f -> f.newName(KEY_FIELD_PREFIX + f.name()).newId(f.id() + KEY_FIELD_ID_START))
                .collect(Collectors.toList());
    }

    public static MergeFunctionFactory<KeyValue> createMergeFunctionFactory(
            TableSchema tableSchema) {
        RowType rowType = tableSchema.logicalRowType();
        Options conf = Options.fromMap(tableSchema.options());
        CoreOptions options = new CoreOptions(conf);
        CoreOptions.MergeEngine mergeEngine = options.mergeEngine();

        switch (mergeEngine) {
            case DEDUPLICATE:
                return DeduplicateMergeFunction.factory(conf);
            case PARTIAL_UPDATE:
                return PartialUpdateMergeFunction.factory(conf, rowType, tableSchema.primaryKeys());
            case AGGREGATE:
                return AggregateMergeFunction.factory(conf, rowType, tableSchema.primaryKeys());
            case FIRST_ROW:
                return FirstRowMergeFunction.factory(conf);
            default:
                throw new UnsupportedOperationException("Unsupported merge engine: " + mergeEngine);
        }
    }

    /** Primary key fields extractor. */
    public static class PrimaryKeyFieldsExtractor implements KeyValueFieldsExtractor {

        private static final long serialVersionUID = 1L;

        public static final PrimaryKeyFieldsExtractor EXTRACTOR = new PrimaryKeyFieldsExtractor();

        private PrimaryKeyFieldsExtractor() {}

        @Override
        public List<DataField> keyFields(TableSchema schema) {
            return addKeyNamePrefix(schema.trimmedPrimaryKeysFields());
        }

        @Override
        public List<DataField> valueFields(TableSchema schema) {
            return schema.fields();
        }
    }

    /**
     * This method checks if a table is properly configured to handle delete operations by primary
     * key upsert. It checks primary key and merge-engine.
     */
    public static void validatePKUpsertDeletable(Table table) {
        if (table.primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "table '%s' can not support delete, because there is no primary key.",
                            table.getClass().getName()));
        }

        Options options = Options.fromMap(table.options());
        CoreOptions.MergeEngine mergeEngine = options.get(MERGE_ENGINE);

        switch (mergeEngine) {
            case DEDUPLICATE:
                return;
            case PARTIAL_UPDATE:
                if (options.get(PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE)
                        || options.get(PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP) != null) {
                    return;
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Merge engine %s doesn't support batch delete by default. To support batch delete, "
                                            + "please set %s to true when there is no %s or set %s.",
                                    mergeEngine,
                                    PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE.key(),
                                    SEQUENCE_GROUP,
                                    PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP));
                }
            case AGGREGATE:
                if (options.get(AGGREGATION_REMOVE_RECORD_ON_DELETE)) {
                    return;
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Merge engine %s doesn't support batch delete by default. To support batch delete, "
                                            + "please set %s to true.",
                                    mergeEngine, AGGREGATION_REMOVE_RECORD_ON_DELETE.key()));
                }
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Merge engine %s can not support batch delete.", mergeEngine));
        }
    }
}
