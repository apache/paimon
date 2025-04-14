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
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of KeyValueDataFileWriter for thin data files. Thin data files only contain
 * _SEQUENCE_NUMBER_, _ROW_KIND_ and value fields.
 */
public class KeyValueThinDataFileWriterImpl extends KeyValueDataFileWriter {

    private final int[] keyStatMapping;

    public KeyValueThinDataFileWriterImpl(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            Function<KeyValue, InternalRow> converter,
            RowType keyType,
            RowType valueType,
            long schemaId,
            int level,
            CoreOptions options,
            FileSource fileSource,
            FileIndexOptions fileIndexOptions,
            boolean isExternalPath) {
        super(
                fileIO,
                context,
                path,
                converter,
                keyType,
                valueType,
                KeyValue.schema(RowType.of(), valueType),
                schemaId,
                level,
                options,
                fileSource,
                fileIndexOptions,
                isExternalPath);
        Map<Integer, Integer> idToIndex = new HashMap<>(valueType.getFieldCount());
        for (int i = 0; i < valueType.getFieldCount(); i++) {
            idToIndex.put(valueType.getFields().get(i).id(), i);
        }
        this.keyStatMapping = new int[keyType.getFieldCount()];
        for (int i = 0; i < keyType.getFieldCount(); i++) {
            keyStatMapping[i] =
                    idToIndex.get(
                            keyType.getFields().get(i).id() - SpecialFields.KEY_FIELD_ID_START);
        }
    }

    /**
     * Fetches the key and value statistics.
     *
     * @param rowStats The row statistics.
     * @return A pair of key statistics and value statistics.
     */
    @Override
    Pair<SimpleColStats[], SimpleColStats[]> fetchKeyValueStats(SimpleColStats[] rowStats) {
        int numKeyFields = keyType.getFieldCount();
        // In thin mode, there is no key stats in rowStats, so we only jump
        // _SEQUENCE_NUMBER_ and _ROW_KIND_ stats. Therefore, the 'from' value is 2.
        SimpleColStats[] valFieldStats = Arrays.copyOfRange(rowStats, 2, rowStats.length);
        // Thin mode on, so need to map value stats to key stats.
        SimpleColStats[] keyStats = new SimpleColStats[numKeyFields];
        for (int i = 0; i < keyStatMapping.length; i++) {
            keyStats[i] = valFieldStats[keyStatMapping[i]];
        }

        return Pair.of(keyStats, valFieldStats);
    }
}
