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

import org.apache.flink.table.store.file.utils.JsonSerdeUtil;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.CoreOptions.BUCKET_KEY;

/** Schema of a table. */
public class TableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    /** System field names. */
    public static final String KEY_FIELD_PREFIX = "_KEY_";

    public static final String VALUE_COUNT = "_VALUE_COUNT";
    public static final String SEQUENCE_NUMBER = "_SEQUENCE_NUMBER";
    public static final String VALUE_KIND = "_VALUE_KIND";
    public static final List<String> SYSTEM_FIELD_NAMES =
            Arrays.asList(VALUE_COUNT, SEQUENCE_NUMBER, VALUE_KIND);

    private final long id;

    private final List<DataField> fields;

    /** Not available from fields, as some fields may have been deleted. */
    private final int highestFieldId;

    private final List<String> partitionKeys;

    private final List<String> primaryKeys;

    private final Map<String, String> options;

    private final String comment;

    public TableSchema(
            long id,
            List<DataField> fields,
            int highestFieldId,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options,
            String comment) {
        this.id = id;
        this.fields = fields;
        this.highestFieldId = highestFieldId;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.options = Collections.unmodifiableMap(options);
        this.comment = comment;

        // try to trim to validate primary keys
        trimmedPrimaryKeys();
    }

    public long id() {
        return id;
    }

    public List<DataField> fields() {
        return fields;
    }

    public List<String> fieldNames() {
        return fields.stream().map(DataField::name).collect(Collectors.toList());
    }

    public int highestFieldId() {
        return highestFieldId;
    }

    public List<String> partitionKeys() {
        return partitionKeys;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public List<String> trimmedPrimaryKeys() {
        if (primaryKeys.size() > 0) {
            Preconditions.checkState(
                    primaryKeys.containsAll(partitionKeys),
                    String.format(
                            "Primary key constraint %s should include all partition fields %s",
                            primaryKeys, partitionKeys));
            List<String> adjusted =
                    primaryKeys.stream()
                            .filter(pk -> !partitionKeys.contains(pk))
                            .collect(Collectors.toList());

            Preconditions.checkState(
                    adjusted.size() > 0,
                    String.format(
                            "Primary key constraint %s should not be same with partition fields %s,"
                                    + " this will result in only one record in a partition",
                            primaryKeys, partitionKeys));

            return adjusted;
        }

        return primaryKeys;
    }

    public Map<String, String> options() {
        return options;
    }

    /** Original bucket keys, maybe empty. */
    public List<String> originalBucketKeys() {
        String key = options.get(BUCKET_KEY.key());
        if (StringUtils.isNullOrWhitespaceOnly(key)) {
            return Collections.emptyList();
        }
        List<String> bucketKeys = Arrays.asList(key.split(","));
        if (!containsAll(fieldNames(), bucketKeys)) {
            throw new RuntimeException(
                    String.format(
                            "Field names %s should contains all bucket keys %s.",
                            fieldNames(), bucketKeys));
        }
        if (bucketKeys.stream().anyMatch(partitionKeys::contains)) {
            throw new RuntimeException(
                    String.format(
                            "Bucket keys %s should not in partition keys %s.",
                            bucketKeys, partitionKeys));
        }
        if (primaryKeys.size() > 0) {
            if (!containsAll(primaryKeys, bucketKeys)) {
                throw new RuntimeException(
                        String.format(
                                "Primary keys %s should contains all bucket keys %s.",
                                primaryKeys, bucketKeys));
            }
        }
        return bucketKeys;
    }

    private boolean containsAll(List<String> all, List<String> contains) {
        return new HashSet<>(all).containsAll(new HashSet<>(contains));
    }

    public String comment() {
        return comment;
    }

    public RowType logicalRowType() {
        return new RowType(fields);
    }

    public RowType logicalPartitionType() {
        return projectedLogicalRowType(partitionKeys);
    }

    public RowType logicalBucketKeyType() {
        List<String> bucketKeys = originalBucketKeys();
        if (bucketKeys.isEmpty()) {
            bucketKeys = trimmedPrimaryKeys();
        }
        if (bucketKeys.isEmpty()) {
            bucketKeys = fieldNames();
        }
        return projectedLogicalRowType(bucketKeys);
    }

    public RowType logicalTrimmedPrimaryKeysType() {
        return projectedLogicalRowType(trimmedPrimaryKeys());
    }

    public List<DataField> trimmedPrimaryKeysFields() {
        return projectedDataFields(trimmedPrimaryKeys());
    }

    public int[] projection(List<String> projectedFieldNames) {
        List<String> fieldNames = fieldNames();
        return projectedFieldNames.stream().mapToInt(fieldNames::indexOf).toArray();
    }

    private List<DataField> projectedDataFields(List<String> projectedFieldNames) {
        List<String> fieldNames = fieldNames();
        return projectedFieldNames.stream()
                .map(k -> fields.get(fieldNames.indexOf(k)))
                .collect(Collectors.toList());
    }

    private RowType projectedLogicalRowType(List<String> projectedFieldNames) {
        return new RowType(projectedDataFields(projectedFieldNames));
    }

    public TableSchema copy(Map<String, String> newOptions) {
        return new TableSchema(
                id, fields, highestFieldId, partitionKeys, primaryKeys, newOptions, comment);
    }

    @Override
    public String toString() {
        return JsonSerdeUtil.toJson(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableSchema tableSchema = (TableSchema) o;
        return Objects.equals(fields, tableSchema.fields)
                && Objects.equals(partitionKeys, tableSchema.partitionKeys)
                && Objects.equals(primaryKeys, tableSchema.primaryKeys)
                && Objects.equals(options, tableSchema.options)
                && Objects.equals(comment, tableSchema.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, partitionKeys, primaryKeys, options, comment);
    }

    public UpdateSchema toUpdateSchema() {
        return new UpdateSchema(logicalRowType(), partitionKeys, primaryKeys, options, comment);
    }

    public static List<DataField> newFields(RowType rowType) {
        return rowType.getFields();
    }

    public static int currentHighestFieldId(List<DataField> fields) {
        Set<Integer> fieldIds = new HashSet<>();
        new org.apache.flink.table.store.types.RowType(fields).collectFieldIds(fieldIds);
        return fieldIds.stream().max(Integer::compareTo).orElse(-1);
    }
}
