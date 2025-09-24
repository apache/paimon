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

package org.apache.paimon.io;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public interface RollingFileWriter<T, R> extends FileWriter<T, List<R>> {

    int CHECK_ROLLING_RECORD_CNT = 1000;

    void writeBundle(BundleRecords records) throws IOException;

    @VisibleForTesting
    static FileWriterContext createFileWriterContext(
            FileFormat fileFormat,
            RowType rowType,
            SimpleColStatsCollector.Factory[] statsCollectors,
            String fileCompression) {
        return new FileWriterContext(
                fileFormat.createWriterFactory(rowType),
                createStatsProducer(fileFormat, rowType, statsCollectors),
                fileCompression);
    }

    static SimpleStatsProducer createStatsProducer(
            FileFormat fileFormat,
            RowType rowType,
            SimpleColStatsCollector.Factory[] statsCollectors) {
        boolean isDisabled =
                Arrays.stream(SimpleColStatsCollector.create(statsCollectors))
                        .allMatch(p -> p instanceof NoneSimpleColStatsCollector);
        if (isDisabled) {
            return SimpleStatsProducer.disabledProducer();
        }
        if (fileFormat instanceof AvroFileFormat) {
            SimpleStatsCollector collector = new SimpleStatsCollector(rowType, statsCollectors);
            return SimpleStatsProducer.fromCollector(collector);
        }
        return SimpleStatsProducer.fromExtractor(
                fileFormat.createStatsExtractor(rowType, statsCollectors).orElse(null));
    }
}
