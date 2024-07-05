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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET_KEY;

/**
 * Schema of a table. Unlike schema, it has more information than {@link Schema}, including schemaId
 * and fieldId.
 */
public class TableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int PAIMON_07_VERSION = 1;
    public static final int PAIMON_08_VERSION = 2;
    public static final int CURRENT_VERSION = 3;

    // version of schema for paimon
    private final int version;

    private final long id;

    private final List<DataField> fields;

    /** Not available from fields, as some fields may have been deleted. */
    private final int highestFieldId;

    private final List<String> partitionKeys;

    private final List<String> primaryKeys;

    private final Map<String, String> options;

    private final @Nullable String comment;

    private final long timeMillis;

    public TableSchema(
            long id,
            List<DataField> fields,
            int highestFieldId,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options,
            @Nullable String comment) {
        this(
                CURRENT_VERSION,
                id,
                fields,
                highestFieldId,
                partitionKeys,
                primaryKeys,
                options,
                comment,
                System.currentTimeMillis());
    }

    public TableSchema(
            int version,
            long id,
            List<DataField> fields,
            int highestFieldId,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options,
            @Nullable String comment,
            long timeMillis) {
        this.version = version;
        this.id = id;
        this.fields = fields;
        this.highestFieldId = highestFieldId;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.options = Collections.unmodifiableMap(options);
        this.comment = comment;
        this.timeMillis = timeMillis;

        // try to trim to validate primary keys
        trimmedPrimaryKeys();

        // try to validate bucket keys
        originalBucketKeys();
    }

    public int version() {
        return version;
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

    public List<String> bucketKeys() {
        List<String> bucketKeys = originalBucketKeys();
        if (bucketKeys.isEmpty()) {
            bucketKeys = trimmedPrimaryKeys();
        }
        return bucketKeys;
    }

    public boolean crossPartitionUpdate() {
        if (primaryKeys.isEmpty() || partitionKeys.isEmpty()) {
            return false;
        }

        return !primaryKeys.containsAll(partitionKeys);
    }

    /** Original bucket keys, maybe empty. */
    private List<String> originalBucketKeys() {
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

    public @Nullable String comment() {
        return comment;
    }

    public long timeMillis() {
        return timeMillis;
    }

    public RowType logicalRowType() {
        return new RowType(fields);
    }

    public RowType logicalPartitionType() {
        return projectedLogicalRowType(partitionKeys);
    }

    public RowType logicalBucketKeyType() {
        return projectedLogicalRowType(bucketKeys());
    }

    public RowType logicalTrimmedPrimaryKeysType() {
        return projectedLogicalRowType(trimmedPrimaryKeys());
    }

    public RowType logicalPrimaryKeysType() {
        return projectedLogicalRowType(primaryKeys());
    }

    public List<DataField> primaryKeysFields() {
        return projectedDataFields(primaryKeys());
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

    public RowType projectedLogicalRowType(List<String> projectedFieldNames) {
        return new RowType(projectedDataFields(projectedFieldNames));
    }

    public TableSchema copy(Map<String, String> newOptions) {
        return new TableSchema(
                version,
                id,
                fields,
                highestFieldId,
                partitionKeys,
                primaryKeys,
                newOptions,
                comment,
                timeMillis);
    }

    public static TableSchema fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, TableSchema.class);
    }

    public static TableSchema fromPath(FileIO fileIO, Path path) {
        try {
            String json = fileIO.readFileUtf8(path);
            return TableSchema.fromJson(json);
        } catch (IOException e) {
            throw new RuntimeException("Fails to read schema from path " + path, e);
        }
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
        return version == tableSchema.version
                && Objects.equals(fields, tableSchema.fields)
                && Objects.equals(partitionKeys, tableSchema.partitionKeys)
                && Objects.equals(primaryKeys, tableSchema.primaryKeys)
                && Objects.equals(options, tableSchema.options)
                && Objects.equals(comment, tableSchema.comment)
                && timeMillis == tableSchema.timeMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                version, fields, partitionKeys, primaryKeys, options, comment, timeMillis);
    }

    public static List<DataField> newFields(RowType rowType) {
        return rowType.getFields();
    }
}
