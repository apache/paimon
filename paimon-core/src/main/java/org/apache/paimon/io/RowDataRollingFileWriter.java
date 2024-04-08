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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

/** {@link RollingFileWriter} for data files containing {@link InternalRow}. */
public class RowDataRollingFileWriter extends RollingFileWriter<InternalRow, DataFileMeta> {

    public RowDataRollingFileWriter(
            FileIO fileIO,
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            LongCounter seqNumCounter,
            String fileCompression,
            FieldStatsCollector.Factory[] statsCollectors,
            CoreOptions coreOptions) {
        super(
                () ->
                        new RowDataFileWriter(
                                fileIO,
                                fileFormat.createWriterFactory(writeSchema),
                                pathFactory,
                                writeSchema,
                                fileFormat instanceof AvroFileFormat
                                        ? null
                                        : fileFormat
                                                .createStatsExtractor(writeSchema, statsCollectors)
                                                .orElse(null),
                                schemaId,
                                seqNumCounter,
                                fileCompression,
                                statsCollectors,
                                coreOptions),
                targetFileSize);
    }
}
