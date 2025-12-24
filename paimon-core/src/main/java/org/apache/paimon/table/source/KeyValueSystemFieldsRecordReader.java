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
 * A decorator for {@link RecordReader} that injects system fields into the output rows for
 * KeyValue-based data sources.
 *
 * <p>This reader wraps a {@code RecordReader<KeyValue>} and produces {@code
 * RecordReader<InternalRow>} with additional system fields extracted from the KeyValue metadata.
 *
 * <p><b>Naming:</b> This class is specifically designed for KeyValue format data (e.g.,
 * MergeFileSplitRead). For InternalRow readers (e.g., RawFileSplitRead), system fields are handled
 * differently in {@link org.apache.paimon.io.DataFileRecordReader} using file metadata.
 *
 * <p><b>Field Ordering:</b> The output schema supports arbitrary field ordering. Internally, fields
 * are assembled as [system fields... + physical fields...], then reordered using {@link
 * ProjectedRow} to match the requested field order.
 *
 * <p><b>Performance:</b> Implementation uses {@link JoinedRow} for zero-copy concatenation of
 * system fields and physical fields, then {@link ProjectedRow} for zero-copy field reordering.
 */
public class KeyValueSystemFieldsRecordReader implements RecordReader<InternalRow> {

    private final RecordReader<KeyValue> wrapped;
    private final List<SystemFieldExtractor> systemFieldExtractors;
    @Nullable private final int[] projection;

    /**
     * Creates a KeyValueSystemFieldsRecordReader.
     *
     * @param wrapped the underlying KeyValue reader
     * @param systemFieldExtractors extractors for system fields, in order
     * @param projection optional projection to reorder fields. If null, fields are in natural order
     *     [system fields... + physical fields...]. If provided, projects from the natural order to
     *     the desired order.
     */
    public KeyValueSystemFieldsRecordReader(
            RecordReader<KeyValue> wrapped,
            List<SystemFieldExtractor> systemFieldExtractors,
            @Nullable int[] projection) {
        this.wrapped = wrapped;
        this.systemFieldExtractors = systemFieldExtractors;
        this.projection = projection;
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        RecordIterator<KeyValue> batch = wrapped.readBatch();
        if (batch == null) {
            return null;
        }
        return new SystemFieldsRecordIterator(batch);
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }

    private class SystemFieldsRecordIterator implements RecordIterator<InternalRow> {

        private final RecordIterator<KeyValue> kvIterator;
        private final JoinedRow joinedRow;
        private final GenericRow systemFieldsRow;
        @Nullable private final ProjectedRow projectedRow;

        private SystemFieldsRecordIterator(RecordIterator<KeyValue> kvIterator) {
            this.kvIterator = kvIterator;
            this.joinedRow = new JoinedRow();
            this.systemFieldsRow = new GenericRow(systemFieldExtractors.size());
            // If projection is provided, use ProjectedRow to reorder fields
            this.projectedRow = projection != null ? ProjectedRow.from(projection) : null;
        }

        @Nullable
        @Override
        public InternalRow next() throws IOException {
            KeyValue kv = kvIterator.next();
            if (kv == null) {
                return null;
            }

            InternalRow value = kv.value();

            // Extract system fields into the reusable row
            for (int i = 0; i < systemFieldExtractors.size(); i++) {
                SystemFieldExtractor extractor = systemFieldExtractors.get(i);
                Object systemValue = extractor.extract(kv);
                systemFieldsRow.setField(i, systemValue);
            }

            // Join system fields first, then physical fields
            // Natural order: [system fields...] + [physical fields...]
            joinedRow.replace(systemFieldsRow, value);
            joinedRow.setRowKind(kv.valueKind());

            // If projection is provided, reorder to match requested order
            if (projectedRow != null) {
                return projectedRow.replaceRow(joinedRow);
            }
            return joinedRow;
        }

        @Override
        public void releaseBatch() {
            kvIterator.releaseBatch();
        }
    }

    /**
     * Wraps a KeyValue reader with system field injection if needed.
     *
     * @param reader the KeyValue reader
     * @param systemFieldExtractors extractors for system fields (empty if no system fields needed)
     * @param projection optional projection to reorder fields from natural order [system fields...
     *     + physical fields...] to desired order
     * @return a reader producing InternalRow with system fields, or a simple unwrapped reader if no
     *     system fields
     */
    public static RecordReader<InternalRow> wrap(
            RecordReader<KeyValue> reader,
            List<SystemFieldExtractor> systemFieldExtractors,
            @Nullable int[] projection) {
        if (systemFieldExtractors.isEmpty()) {
            // No system fields, use the default unwrap logic
            return KeyValueTableRead.unwrap(reader);
        }
        return new KeyValueSystemFieldsRecordReader(reader, systemFieldExtractors, projection);
    }

    // ========== Internal Extractor Interface ==========

    /**
     * Internal interface for extracting system fields from {@link KeyValue} objects.
     *
     * <p>System fields are metadata fields like {@code _SEQUENCE_NUMBER}, {@code _LEVEL}, {@code
     * rowkind} that are derived from the KeyValue container itself rather than the stored data.
     *
     * <p><b>Note:</b> This interface is specifically for KeyValue-based extraction. For InternalRow
     * readers (e.g., RawFileSplitRead), system fields are handled differently in {@link
     * org.apache.paimon.io.DataFileRecordReader} using file metadata.
     *
     * <p>All field definitions are sourced from {@link SpecialFields} to maintain consistency
     * across the codebase.
     *
     * <p>Each extractor is stateless and thread-safe.
     */
    @FunctionalInterface
    public interface SystemFieldExtractor {

        /**
         * Extracts the system field value from a KeyValue object.
         *
         * @param kv the KeyValue to extract from
         * @return the extracted value, or null if not applicable
         */
        @Nullable
        Object extract(KeyValue kv);

        // ========== Built-in Extractors ==========

        /**
         * Extractor for {@code _SEQUENCE_NUMBER} system field.
         *
         * <p>Extracts the sequence number from KeyValue metadata.
         */
        SystemFieldExtractor SEQUENCE_NUMBER = kv -> kv.sequenceNumber();

        /**
         * Extractor for {@code rowkind} system field (used in AuditLogTable).
         *
         * <p>Extracts the row kind from KeyValue's valueKind.
         */
        SystemFieldExtractor ROW_KIND = kv -> BinaryString.fromString(kv.valueKind().shortString());

        /**
         * Extractor for {@code _LEVEL} system field (LSM tree level).
         *
         * <p>Note: Currently not extractable from KeyValue at read time. This is a placeholder for
         * future implementation where level information would need to be tracked through the read
         * path.
         */
        SystemFieldExtractor LEVEL = kv -> null; // TODO: Level information needs to be propagated

        /**
         * Extractor for {@code _ROW_ID} system field.
         *
         * <p>Note: ROW_ID is typically handled by DataFileRecordReader for InternalRow-based
         * readers. This extractor is provided for completeness but may not be used in KeyValue
         * scenarios.
         */
        SystemFieldExtractor ROW_ID =
                kv -> null; // ROW_ID is computed from file metadata, not available in KeyValue
    }

    // ========== Registry ==========

    /** Registry for system field extractors. */
    private static final Map<String, SystemFieldExtractor> EXTRACTOR_REGISTRY = new HashMap<>();

    static {
        // Register all extractors that can be used with KeyValue
        EXTRACTOR_REGISTRY.put(
                SpecialFields.SEQUENCE_NUMBER.name(), SystemFieldExtractor.SEQUENCE_NUMBER);
        EXTRACTOR_REGISTRY.put(SpecialFields.ROW_KIND.name(), SystemFieldExtractor.ROW_KIND);
        EXTRACTOR_REGISTRY.put(SpecialFields.LEVEL.name(), SystemFieldExtractor.LEVEL);
        EXTRACTOR_REGISTRY.put(SpecialFields.ROW_ID.name(), SystemFieldExtractor.ROW_ID);
    }

    /**
     * Gets an extractor by field name.
     *
     * @param fieldName the system field name
     * @return the extractor, or null if not a registered system field
     */
    @Nullable
    public static SystemFieldExtractor getExtractor(String fieldName) {
        return EXTRACTOR_REGISTRY.get(fieldName);
    }
}
