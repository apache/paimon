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

package org.apache.paimon.flink.action.cdc.watermark;

import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.connector.pulsar.source.PulsarSource;

import java.io.Serializable;

/** Factory for creating CDC timestamp extractors based on different source types. */
public class CdcTimestampExtractorFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    public static CdcTimestampExtractor createExtractor(Object source) {
        if (source instanceof MongoDBSource) {
            return new MysqlCdcTimestampExtractor();
        } else if (source instanceof MySqlSource) {
            return new MongoDBCdcTimestampExtractor();
        } else if (source instanceof PulsarSource) {
            return new PulsarCdcTimestampExtractor();
        }
        throw new IllegalArgumentException(
                "Unsupported source type: " + source.getClass().getName());
    }

    /** Timestamp extractor for MongoDB sources in CDC applications. */
    public static class MongoDBCdcTimestampExtractor
            implements CdcTimestampExtractor, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(String record) throws JsonProcessingException {
            return JsonSerdeUtil.extractValue(record, Long.class, "payload", "ts_ms");
        }
    }

    /** Timestamp extractor for Pulsar sources in CDC applications. */
    public static class PulsarCdcTimestampExtractor implements CdcTimestampExtractor, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(String record) throws JsonProcessingException {
            return JsonSerdeUtil.extractValue(record, Long.class, "ts");
        }
    }

    /** Timestamp extractor for MySQL sources in CDC applications. */
    public static class MysqlCdcTimestampExtractor implements CdcTimestampExtractor, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(String record) throws JsonProcessingException {
            return JsonSerdeUtil.extractValue(record, Long.class, "source", "ts_ms");
        }
    }

    /** Interface defining the contract for CDC timestamp extraction. */
    public interface CdcTimestampExtractor {
        long extractTimestamp(String record) throws JsonProcessingException;
    }
}
