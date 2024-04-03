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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** A change message contains schema and data. */
@Experimental
public class RichCdcRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CdcRecord cdcRecord;
    private final List<DataField> fields;

    public RichCdcRecord(CdcRecord cdcRecord, List<DataField> fields) {
        this.cdcRecord = cdcRecord;
        this.fields = fields;
    }

    public boolean hasPayload() {
        return !cdcRecord.fields().isEmpty();
    }

    public RowKind kind() {
        return cdcRecord.kind();
    }

    public List<DataField> fields() {
        return fields;
    }

    public CdcRecord toCdcRecord() {
        return cdcRecord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RichCdcRecord that = (RichCdcRecord) o;
        return cdcRecord == that.cdcRecord && Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cdcRecord, fields);
    }

    @Override
    public String toString() {
        return "{" + "cdcRecord=" + cdcRecord + ", fields=" + fields + '}';
    }

    public static Builder builder(RowKind kind) {
        return new Builder(kind, new AtomicInteger(-1));
    }

    /** Builder for {@link RichCdcRecord}. */
    public static class Builder {

        private final RowKind kind;
        private final AtomicInteger fieldId;
        private final List<DataField> fields = new ArrayList<>();
        private final Map<String, String> fieldValues = new HashMap<>();

        public Builder(RowKind kind, AtomicInteger fieldId) {
            this.kind = kind;
            this.fieldId = fieldId;
        }

        public Builder field(String name, DataType type, String value) {
            return field(name, type, value, null);
        }

        public Builder field(
                String name, DataType type, String value, @Nullable String description) {
            fields.add(new DataField(fieldId.incrementAndGet(), name, type, description));
            fieldValues.put(name, value);
            return this;
        }

        public RichCdcRecord build() {
            return new RichCdcRecord(new CdcRecord(kind, fieldValues), fields);
        }
    }
}
