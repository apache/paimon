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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A wrapper for {@link RecordReader} that injects special fields into the output rows for
 * KeyValue-based data sources.
 *
 * <p>This reader wraps a {@code RecordReader<KeyValue>} and produces {@code
 * RecordReader<InternalRow>} with additional special fields extracted from the KeyValue metadata.
 *
 * <p><b>Naming:</b> This class is specifically designed for KeyValue format data (e.g.,
 * MergeFileSplitRead). For InternalRow readers (e.g., RawFileSplitRead), special fields are handled
 * differently in {@link org.apache.paimon.io.DataFileRecordReader} using file metadata.
 *
 * <p><b>Field Ordering:</b> The output schema supports arbitrary field ordering. Internally, fields
 * are assembled as [special fields... + physical fields...], then reordered using {@link
 * ProjectedRow} to match the requested field order.
 *
 * <p><b>Performance:</b> Implementation uses {@link JoinedRow} for zero-copy concatenation of
 * special fields and physical fields, then {@link ProjectedRow} for zero-copy field reordering.
 */
public class KeyValueSpecialFieldsRecordReader implements RecordReader<InternalRow> {

    private final RecordReader<KeyValue> wrapped;
    private final List<SpecialFieldExtractor> specialFieldExtractors;
    @Nullable private final int[] projection;

    /**
     * Creates a KeyValueSpecialFieldsRecordReader.
     *
     * @param wrapped the underlying KeyValue reader
     * @param specialFieldExtractors extractors for special fields, in order
     * @param projection optional projection to reorder fields. If null, fields are in natural order
     *     [special fields... + physical fields...]. If provided, projects from the natural order to
     *     the desired order.
     */
    public KeyValueSpecialFieldsRecordReader(
            RecordReader<KeyValue> wrapped,
            List<SpecialFieldExtractor> specialFieldExtractors,
            @Nullable int[] projection) {
        this.wrapped = wrapped;
        this.specialFieldExtractors = specialFieldExtractors;
        this.projection = projection;
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        RecordIterator<KeyValue> batch = wrapped.readBatch();
        if (batch == null) {
            return null;
        }
        return new SpecialFieldsRecordIterator(batch);
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }

    private class SpecialFieldsRecordIterator implements RecordIterator<InternalRow> {

        private final RecordIterator<KeyValue> kvIterator;
        private final GenericRow specialFieldsRow;

        private SpecialFieldsRecordIterator(RecordIterator<KeyValue> kvIterator) {
            this.kvIterator = kvIterator;
            this.specialFieldsRow = new GenericRow(specialFieldExtractors.size());
        }

        @Nullable
        @Override
        public InternalRow next() throws IOException {
            KeyValue kv = kvIterator.next();
            if (kv == null) {
                return null;
            }

            InternalRow value = kv.value();

            // Extract special fields into the reusable row
            for (int i = 0; i < specialFieldExtractors.size(); i++) {
                SpecialFieldExtractor extractor = specialFieldExtractors.get(i);
                Object specialValue = extractor.extract(kv);
                specialFieldsRow.setField(i, specialValue);
            }

            // Join special fields first, then physical fields
            // Natural order: [special fields...] + [physical fields...]
            JoinedRow joinedRow = new JoinedRow();
            joinedRow.replace(specialFieldsRow, value);
            joinedRow.setRowKind(kv.valueKind());

            // If projection is provided, reorder to match requested order
            if (projection != null) {
                ProjectedRow projectedRow = ProjectedRow.from(projection);
                return projectedRow.replaceRow(joinedRow);
            }
            return joinedRow;
        }

        @Override
        public void releaseBatch() {
            kvIterator.releaseBatch();
        }
    }

    // ========== Internal Extractor Interface ==========

    /**
     * Internal interface for extracting special fields from {@link KeyValue} objects.
     *
     * <p>special fields are metadata fields like {@code _SEQUENCE_NUMBER}, {@code _LEVEL}, {@code
     * rowkind} that are derived from the KeyValue container itself rather than the stored data.
     *
     * <p><b>Note:</b> This interface is specifically for KeyValue-based extraction. For InternalRow
     * readers (e.g., RawFileSplitRead), special fields are handled differently in {@link
     * org.apache.paimon.io.DataFileRecordReader} using file metadata.
     *
     * <p>All field definitions are sourced from {@link SpecialFields} to maintain consistency
     * across the codebase.
     *
     * <p>Each extractor is stateless and thread-safe.
     */
    @FunctionalInterface
    public interface SpecialFieldExtractor {

        /**
         * Extracts the special field value from a KeyValue object.
         *
         * @param kv the KeyValue to extract from
         * @return the extracted value, or null if not applicable
         */
        @Nullable
        Object extract(KeyValue kv);

        // ========== Built-in Extractors ==========

        /**
         * Extractor for {@code _SEQUENCE_NUMBER} special field.
         *
         * <p>Extracts the sequence number from KeyValue metadata.
         */
        SpecialFieldExtractor SEQUENCE_NUMBER = KeyValue::sequenceNumber;

        /**
         * Extractor for {@code rowkind} special field (used in AuditLogTable).
         *
         * <p>Extracts the row kind from KeyValue's valueKind.
         */
        SpecialFieldExtractor ROW_KIND =
                kv -> BinaryString.fromString(kv.valueKind().shortString());

        /**
         * Extractor for {@code _LEVEL} special field (LSM tree level).
         *
         * <p>Note: Currently not extractable from KeyValue at read time. This is a placeholder for
         * future implementation where level information would need to be tracked through the read
         * path.
         */
        SpecialFieldExtractor LEVEL =
                kv -> {
                    throw new UnsupportedOperationException(
                            "LEVEL special field is not yet supported for KeyValue-based reads. "
                                    + "Level information needs to be propagated through the read path.");
                };

        /**
         * Extractor for {@code _ROW_ID} special field.
         *
         * <p>Note: ROW_ID is typically handled by DataFileRecordReader for InternalRow-based
         * readers. This extractor is provided for completeness but may not be used in KeyValue
         * scenarios.
         */
        SpecialFieldExtractor ROW_ID =
                kv -> {
                    throw new UnsupportedOperationException(
                            "ROW_ID special field is not supported for KeyValue-based reads. "
                                    + "ROW_ID is computed from file metadata in DataFileRecordReader.");
                };
    }

    // ========== Registry ==========

    /** Registry for special field extractors. */
    private static final Map<String, SpecialFieldExtractor> EXTRACTOR_REGISTRY = new HashMap<>();

    static {
        // Register all extractors that can be used with KeyValue
        EXTRACTOR_REGISTRY.put(
                SpecialFields.SEQUENCE_NUMBER.name(), SpecialFieldExtractor.SEQUENCE_NUMBER);
        EXTRACTOR_REGISTRY.put(SpecialFields.ROW_KIND.name(), SpecialFieldExtractor.ROW_KIND);
        EXTRACTOR_REGISTRY.put(SpecialFields.LEVEL.name(), SpecialFieldExtractor.LEVEL);
        EXTRACTOR_REGISTRY.put(SpecialFields.ROW_ID.name(), SpecialFieldExtractor.ROW_ID);
    }

    /**
     * Gets an extractor by field name.
     *
     * @param fieldName the special field name
     * @return the extractor, or null if not a registered special field
     */
    @Nullable
    public static SpecialFieldExtractor getExtractor(String fieldName) {
        return EXTRACTOR_REGISTRY.get(fieldName);
    }
}
