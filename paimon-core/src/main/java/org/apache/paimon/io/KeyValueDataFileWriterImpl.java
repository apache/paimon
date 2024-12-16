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
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.function.Function;

/** Write data files containing {@link KeyValue}s. */
public class KeyValueDataFileWriterImpl extends KeyValueDataFileWriter {

    public KeyValueDataFileWriterImpl(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            Function<KeyValue, InternalRow> converter,
            RowType keyType,
            RowType valueType,
            @Nullable SimpleStatsExtractor simpleStatsExtractor,
            long schemaId,
            int level,
            String compression,
            CoreOptions options,
            FileSource fileSource,
            FileIndexOptions fileIndexOptions,
            Path defaultWriteRootPath) {
        super(
                fileIO,
                factory,
                path,
                converter,
                keyType,
                valueType,
                KeyValue.schema(keyType, valueType),
                simpleStatsExtractor,
                schemaId,
                level,
                compression,
                options,
                fileSource,
                fileIndexOptions,
                defaultWriteRootPath);
    }

    @Override
    Pair<SimpleColStats[], SimpleColStats[]> fetchKeyValueStats(SimpleColStats[] rowStats) {
        int numKeyFields = keyType.getFieldCount();
        return Pair.of(
                Arrays.copyOfRange(rowStats, 0, numKeyFields),
                Arrays.copyOfRange(rowStats, numKeyFields + 2, rowStats.length));
    }
}
