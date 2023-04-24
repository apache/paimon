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

package org.apache.paimon.manifest;

import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SegmentsCache;

import javax.annotation.Nullable;

/** This class read partitions from manifest file, only decode the partition related bytes. */
public class SimpleManifestReader extends ObjectsFile<SimpleManifestEntry> {

    private final RowType partitionType;

    public SimpleManifestReader(
            FileIO fileIO,
            ObjectSerializer<SimpleManifestEntry> serializer,
            RowType partitionType,
            FormatReaderFactory readerFactory,
            PathFactory pathFactory,
            @Nullable SegmentsCache<String> cache) {
        super(fileIO, serializer, readerFactory, pathFactory, cache);
        this.partitionType = partitionType;
    }

    public RowType getPartitionType() {
        return partitionType;
    }
}
