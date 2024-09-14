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
import org.apache.paimon.utils.ObjectUtils;

import java.util.Collections;
import java.util.List;

/** Testing {@link EventParser} for {@link TestCdcEvent}. */
public class TestCdcEventParser implements EventParser<TestCdcEvent> {

    private TestCdcEvent raw;

    @Override
    public void setRawEvent(TestCdcEvent raw) {
        this.raw = raw;
    }

    @Override
    public String parseTableName() {
        return raw.tableName();
    }

    @Override
    public List<DataField> parseSchemaChange() {
        return ObjectUtils.coalesce(raw.updatedDataFields(), Collections.emptyList());
    }

    @Override
    public List<CdcRecord> parseRecords() {
        return ObjectUtils.coalesce(raw.records(), Collections.emptyList());
    }

    @Override
    public void evalComputedColumns(List<DataField> fields) {}
}
