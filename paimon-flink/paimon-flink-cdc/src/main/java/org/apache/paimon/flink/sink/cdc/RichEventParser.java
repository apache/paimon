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

import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/** A {@link EventParser} for {@link RichCdcRecord}. */
public class RichEventParser implements EventParser<RichCdcRecord> {

    private RichCdcRecord record;

    private final LinkedHashMap<String, DataField> previousDataFields = new LinkedHashMap<>();

    @Override
    public void setRawEvent(RichCdcRecord rawEvent) {
        this.record = rawEvent;
    }

    @Override
    public List<DataField> parseSchemaChange() {
        List<DataField> change = new ArrayList<>();
        record.fields()
                .forEach(
                        dataField -> {
                            DataField previous = previousDataFields.get(dataField.name());
                            // When the order of the same field is different, its ID may also be
                            // different,
                            // so the comparison should not include the ID.
                            if (!DataField.dataFieldEqualsIgnoreId(previous, dataField)) {
                                previousDataFields.put(dataField.name(), dataField);
                                change.add(dataField);
                            }
                        });
        return change;
    }

    @Override
    public List<CdcRecord> parseRecords() {
        if (record.hasPayload()) {
            return Collections.singletonList(record.toCdcRecord());
        } else {
            return Collections.emptyList();
        }
    }
}
