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

package org.apache.paimon.table.source.splitread;

import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.DataEvolutionSplitRead;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.LazyField;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;

/** A {@link SplitReadProvider} to create {@link DataEvolutionSplitRead}. */
public class DataEvolutionSplitReadProvider implements SplitReadProvider {

    private final LazyField<DataEvolutionSplitRead> splitRead;

    public DataEvolutionSplitReadProvider(
            Supplier<DataEvolutionSplitRead> supplier, SplitReadConfig splitReadConfig) {
        this.splitRead =
                new LazyField<>(
                        () -> {
                            DataEvolutionSplitRead read = supplier.get();
                            splitReadConfig.config(read);
                            return read;
                        });
    }

    @Override
    public boolean match(Split split, Context context) {
        DataSplit dataSplit;
        if (split instanceof DataSplit) {
            dataSplit = (DataSplit) split;
        } else if (split instanceof IndexedSplit) {
            dataSplit = ((IndexedSplit) split).dataSplit();
        } else {
            return false;
        }

        List<DataFileMeta> files = dataSplit.dataFiles();
        if (files.size() < 2) {
            return false;
        }

        Set<Long> firstRowIds = new HashSet<>();
        for (DataFileMeta file : files) {
            if (isBlobFile(file.fileName())) {
                return true;
            }
            Long current = file.firstRowId();
            if (current == null
                    || !file.fileSource().isPresent()
                    || file.fileSource().get() != FileSource.APPEND) {
                return false;
            }

            firstRowIds.add(current);
        }

        // If all files have a distinct first row id, we don't need to merge fields
        return firstRowIds.size() != files.size();
    }

    @Override
    public LazyField<DataEvolutionSplitRead> get() {
        return splitRead;
    }
}
