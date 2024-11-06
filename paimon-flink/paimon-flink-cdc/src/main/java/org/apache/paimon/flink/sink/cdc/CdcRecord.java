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
import java.util.Map;
import java.util.Objects;

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
            newData.put(entry.getKey().toLowerCase(), entry.getValue());
        }
        return new CdcRecord(kind, newData);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CdcRecord)) {
            return false;
        }

        CdcRecord that = (CdcRecord) o;
        return Objects.equals(kind, that.kind) && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, data);
    }

    @Override
    public String toString() {
        return kind.shortString() + " " + data;
    }
}
