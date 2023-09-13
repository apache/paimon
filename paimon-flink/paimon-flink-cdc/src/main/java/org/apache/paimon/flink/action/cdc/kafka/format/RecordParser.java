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

package org.apache.paimon.flink.action.cdc.kafka.format;

import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Provides an interface for parsing messages of various formats into {@link RichCdcMultiplexRecord}
 * objects.
 */
public interface RecordParser extends Serializable {

    void open();

    List<RichCdcMultiplexRecord> extractRecords(String topic, byte[] key, byte[] value)
            throws IOException;

    void validateFormat();

    void setPrimaryField();

    void setDataField();

    Schema getKafkaSchema(String topic, byte[] key, byte[] value);
}
