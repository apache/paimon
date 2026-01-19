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

package org.apache.paimon.data.variant;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utils for marking and identifying variant-originated RowType. Uses description field in DataField
 * to encode variant metadata.
 *
 * <p>Description format: __VARIANT_METADATA&lt;path&gt;;&lt;failOnError&gt;;&lt;timeZoneId&gt;
 *
 * <p>Example: __VARIANT_METADATA$.a.b;true;UTC
 */
public class VariantMetadataUtils {

    public static final String METADATA_KEY = "__VARIANT_METADATA";
    public static final String DELIMITER = ";";

    /** Build variant metadata description string. */
    public static String buildVariantMetadata(String path, boolean failOnError, String timeZoneId) {
        return METADATA_KEY + path + DELIMITER + failOnError + DELIMITER + timeZoneId;
    }

    public static String buildVariantMetadata(String path) {
        return buildVariantMetadata(path, true, "UTC");
    }

    /** Check if a RowType is originated from a variant. */
    public static boolean isVariantRowType(DataType dataType) {
        if (!(dataType instanceof RowType)) {
            return false;
        }

        RowType rowType = (RowType) dataType;
        if (rowType.getFields().isEmpty()) {
            return false;
        }
        for (DataField f : rowType.getFields()) {
            if (f.description() == null || !f.description().startsWith(METADATA_KEY)) {
                return false;
            }
        }
        return true;
    }

    /** Extract the path from variant metadata description. */
    public static String path(String description) {
        return splitDescription(description)[0];
    }

    /** Extract the failOnError flag from variant metadata description. */
    public static boolean failOnError(String description) {
        return Boolean.parseBoolean(splitDescription(description)[1]);
    }

    /** Extract the time zone id from variant metadata description. */
    public static ZoneId timeZoneId(String description) {
        return ZoneId.of(splitDescription(description)[2]);
    }

    private static String[] splitDescription(String description) {
        return description.substring(METADATA_KEY.length()).split(DELIMITER);
    }

    /** Builder for creating variant row types with metadata. */
    public static class VariantRowTypeBuilder {

        private final List<DataField> fields = new ArrayList<>();

        private final boolean isNullable;
        private final AtomicInteger fieldId;

        private VariantRowTypeBuilder(boolean isNullable, AtomicInteger fieldId) {
            this.isNullable = isNullable;
            this.fieldId = fieldId;
        }

        public VariantRowTypeBuilder field(DataType type, String path) {
            return field(type, path, true, "UTC");
        }

        public VariantRowTypeBuilder field(
                DataType type, String path, boolean failOnError, String timeZoneId) {
            int id = fieldId.incrementAndGet();
            fields.add(
                    new DataField(
                            id,
                            String.valueOf(id),
                            type,
                            VariantMetadataUtils.buildVariantMetadata(
                                    path, failOnError, timeZoneId)));
            return this;
        }

        public static VariantRowTypeBuilder builder() {
            return builder(true);
        }

        public static VariantRowTypeBuilder builder(boolean isNullable) {
            return new VariantRowTypeBuilder(isNullable, new AtomicInteger(-1));
        }

        public RowType build() {
            return new RowType(isNullable, fields);
        }
    }
}
