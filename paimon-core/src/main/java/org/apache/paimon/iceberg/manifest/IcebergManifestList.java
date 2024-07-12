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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;

/**
 * This file includes several Iceberg {@link IcebergManifestFileMeta}s, representing the additional
 * changes since last snapshot.
 */
public class IcebergManifestList extends ObjectsFile<IcebergManifestFileMeta> {

    public IcebergManifestList(
            FileIO fileIO,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            String compression,
            PathFactory pathFactory) {
        super(
                fileIO,
                new IcebergManifestFileMetaSerializer(),
                readerFactory,
                writerFactory,
                compression,
                pathFactory,
                null);
    }
}
