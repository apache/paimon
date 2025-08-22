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

package org.apache.paimon.deletionvectors.append;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.source.DeletionFile;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Helper for {@link BaseAppendDeleteFileMaintainer}. */
public class AppendDeletionFileMaintainerHelper {

    public static AppendDeleteFileMaintainer fromDeletionFiles(
            IndexFileHandler indexFileHandler,
            BinaryRow partition,
            Map<String, DeletionFile> deletionFiles) {
        List<String> touchedIndexFileNames =
                deletionFiles.values().stream()
                        .map(deletionFile -> new Path(deletionFile.path()).getName())
                        .distinct()
                        .collect(Collectors.toList());
        List<IndexManifestEntry> manifests =
                indexFileHandler.scanEntries().stream()
                        .filter(
                                indexManifestEntry ->
                                        touchedIndexFileNames.contains(
                                                indexManifestEntry.indexFile().fileName()))
                        .collect(Collectors.toList());
        return new AppendDeleteFileMaintainer(
                indexFileHandler.dvIndex(), partition, manifests, deletionFiles);
    }
}
