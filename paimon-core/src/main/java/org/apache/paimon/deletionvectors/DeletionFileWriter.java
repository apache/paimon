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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.VERSION_ID_V1;

/** Writer to write deletion file. */
public class DeletionFileWriter implements Closeable {

    private final Path path;
    private final boolean isExternalPath;
    private final DataOutputStream out;
    private final LinkedHashMap<String, DeletionVectorMeta> dvMetas;

    public DeletionFileWriter(IndexPathFactory pathFactory, FileIO fileIO) throws IOException {
        this.path = pathFactory.newPath();
        this.isExternalPath = pathFactory.isExternalPath();
        this.out = new DataOutputStream(fileIO.newOutputStream(path, true));
        out.writeByte(VERSION_ID_V1);
        this.dvMetas = new LinkedHashMap<>();
    }

    public long getPos() {
        return out.size();
    }

    public void write(String key, DeletionVector deletionVector) throws IOException {
        int start = out.size();
        int length = deletionVector.serializeTo(out);
        dvMetas.put(
                key, new DeletionVectorMeta(key, start, length, deletionVector.getCardinality()));
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    public IndexFileMeta result() {
        return new IndexFileMeta(
                DELETION_VECTORS_INDEX,
                path.getName(),
                getPos(),
                dvMetas.size(),
                dvMetas,
                isExternalPath ? path.toString() : null);
    }
}
