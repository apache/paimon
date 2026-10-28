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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.types.RowKind;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** A data change message from the CDC source. */
@Experimental
public class CdcRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private RowKind kind;

    // field name -> value
    private final Map<String, String> data;

    public CdcRecord(RowKind kind, Map<String, String> data) {
        this.kind = kind;
        this.data = data;
    }

    /**
     * Creates a record whose specified fields contain Base64-encoded binary values.
     *
     * <p>Non-null values of the specified fields must already be Base64-encoded. This method only
     * records their encoding; it does not encode or decode values. Unmarked fields retain the usual
     * CDC string conversion, including text mapped to VARBINARY.
     */
    public static CdcRecord withBase64EncodedFields(
            RowKind kind, Map<String, String> data, Set<String> base64EncodedFields) {
        return base64EncodedFields.isEmpty()
                ? new CdcRecord(kind, data)
                : new BinaryCdcRecord(kind, data, base64EncodedFields);
    }

    /** Names of fields whose non-null values contain Base64-encoded binary data. */
    public Set<String> base64EncodedFields() {
        return Collections.emptySet();
    }

    public static CdcRecord emptyRecord() {
        return new CdcRecord(RowKind.INSERT, Collections.emptyMap());
    }

    public RowKind kind() {
        return kind;
    }

    public Map<String, String> data() {
        return data;
    }

    public CdcRecord fieldNameLowerCase() {
        Map<String, String> newData = new HashMap<>();
        for (Map.Entry<String, String> entry : data.entrySet()) {
            // Locale.ROOT: must match the schema-side toLowerCaseIfNeed conversion;
            // a Turkish default locale would lowercase 'I' to a dotless glyph and
            // silently null out the column on the record-schema join
            newData.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
        }
        Set<String> newBase64EncodedFields = new HashSet<>();
        for (String field : base64EncodedFields()) {
            newBase64EncodedFields.add(field.toLowerCase(Locale.ROOT));
        }
        return withBase64EncodedFields(kind, newData, newBase64EncodedFields);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CdcRecord)) {
            return false;
        }

        CdcRecord that = (CdcRecord) o;
        return Objects.equals(kind, that.kind)
                && Objects.equals(data, that.data)
                && Objects.equals(base64EncodedFields(), that.base64EncodedFields());
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, data, base64EncodedFields());
    }

    // Keep the serialized fields of ordinary CdcRecord unchanged. Only records with explicitly
    // encoded binary values need the additional metadata.
    private static class BinaryCdcRecord extends CdcRecord {

        private static final long serialVersionUID = 1L;

        // Keep the serialized field name unchanged.
        private final Set<String> binaryFields;

        private BinaryCdcRecord(
                RowKind kind, Map<String, String> data, Set<String> base64EncodedFields) {
            super(kind, data);
            this.binaryFields = new HashSet<>(base64EncodedFields);
        }

        @Override
        public Set<String> base64EncodedFields() {
            return binaryFields;
        }
    }

    @Override
    public String toString() {
        return kind.shortString() + " " + data;
    }
}
