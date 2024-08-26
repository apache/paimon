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

package org.apache.paimon.flink.action.cdc.format.ogg;

import org.apache.paimon.flink.action.cdc.format.AbstractJsonDataFormat;
import org.apache.paimon.flink.action.cdc.format.RecordParserFactory;

/**
 * Supports the message queue's ogg json data format and provides definitions for the message
 * queue's record json deserialization class and parsing class {@link OggRecordParser}.
 */
public class OggDataFormat extends AbstractJsonDataFormat {

    @Override
    protected RecordParserFactory parser() {
        return OggRecordParser::new;
    }
}
