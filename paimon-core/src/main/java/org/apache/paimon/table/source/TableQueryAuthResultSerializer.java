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

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.rest.RESTApi;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Serializer for {@link TableQueryAuthResult}. */
public class TableQueryAuthResultSerializer {
    public static void serialize(TableQueryAuthResult authResult, DataOutputView out)
            throws IOException {
        // Serialize row filter
        if (authResult.rowFilter() != null) {
            out.writeBoolean(true);
            String predicateJson = RESTApi.toJson(authResult.rowFilter());
            out.writeUTF(predicateJson);
        } else {
            out.writeBoolean(false);
        }

        // Serialize column masking
        Map<String, Transform> columnMasking = authResult.columnMasking();
        out.writeInt(columnMasking.size());
        for (Map.Entry<String, Transform> entry : columnMasking.entrySet()) {
            out.writeUTF(entry.getKey());
            String transformJson = RESTApi.toJson(entry.getValue());
            out.writeUTF(transformJson);
        }
    }

    public static TableQueryAuthResult deserialize(DataInputView in) throws IOException {
        // Deserialize row filter
        Predicate rowFilter = null;
        if (in.readBoolean()) {
            String predicateJson = in.readUTF();
            rowFilter = RESTApi.fromJson(predicateJson, Predicate.class);
        }

        // Deserialize column masking
        int maskingSize = in.readInt();
        Map<String, Transform> columnMasking = new HashMap<>(maskingSize);
        for (int i = 0; i < maskingSize; i++) {
            String columnName = in.readUTF();
            String transformJson = in.readUTF();
            Transform transform = RESTApi.fromJson(transformJson, Transform.class);
            columnMasking.put(columnName, transform);
        }

        return new TableQueryAuthResult(rowFilter, columnMasking);
    }
}
