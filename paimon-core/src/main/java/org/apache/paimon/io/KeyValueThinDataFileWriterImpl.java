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
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of KeyValueDataFileWriter for thin data files. Thin data files only contain
 * _SEQUENCE_NUMBER_, _ROW_KIND_ and value fields.
 */
public class KeyValueThinDataFileWriterImpl extends KeyValueDataFileWriter {

    private final int[] keyStatMapping;

    /**
     * Constructs a KeyValueThinDataFileWriterImpl.
     *
     * @param fileIO The file IO interface.
     * @param factory The format writer factory.
     * @param path The path to the file.
     * @param converter The function to convert KeyValue to InternalRow.
     * @param keyType The row type of the key.
     * @param valueType The row type of the value.
     * @param simpleStatsExtractor The simple stats extractor, can be null.
     * @param schemaId The schema ID.
     * @param level The level.
     * @param compression The compression type.
     * @param options The core options.
     * @param fileSource The file source.
     * @param fileIndexOptions The file index options.
     */
    public KeyValueThinDataFileWriterImpl(
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
            FileIndexOptions fileIndexOptions) {
        super(
                fileIO,
                factory,
                path,
                converter,
                keyType,
                valueType,
                simpleStatsExtractor,
                schemaId,
                level,
                compression,
                options,
                fileSource,
                fileIndexOptions);
        Map<Integer, Integer> idToIndex = new HashMap<>();
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
        int numValueFields = valueType.getFieldCount();

        SimpleColStats[] keyStats = new SimpleColStats[numKeyFields];
        SimpleColStats[] valFieldStats = new SimpleColStats[numValueFields];

        // In thin mode, there is no key stats in rowStats, so we only jump
        // _SEQUNCE_NUMBER_ and _ROW_KIND_ stats. Therefore, the 'from' value is 2.
        System.arraycopy(rowStats, 2, valFieldStats, 0, numValueFields);
        // Thin mode on, so need to map value stats to key stats.
        for (int i = 0; i < keyStatMapping.length; i++) {
            keyStats[i] = valFieldStats[keyStatMapping[i]];
        }

        return Pair.of(keyStats, valFieldStats);
    }
}
