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

package org.apache.paimon.table.source;

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Index file for data file. */
public class IndexFile {

    private final String path;

    public IndexFile(String path) {
        this.path = path;
    }

    public static void serialize(DataOutputView out, @Nullable IndexFile indexFile)
            throws IOException {
        if (indexFile == null) {
            out.write(0);
        } else {
            out.write(1);
            out.writeUTF(indexFile.path);
        }
    }

    public static void serializeList(DataOutputView out, List<IndexFile> files) throws IOException {
        out.writeInt(files.size());
        for (IndexFile file : files) {
            serialize(out, file);
        }
    }

    @Nullable
    public static IndexFile deserialize(DataInputView in) throws IOException {
        return in.readByte() == 1 ? new IndexFile(in.readUTF()) : null;
    }

    public static List<IndexFile> deserializeList(DataInputView in) throws IOException {
        List<IndexFile> files = new ArrayList<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            files.add(IndexFile.deserialize(in));
        }
        return files;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IndexFile)) {
            return false;
        }

        IndexFile other = (IndexFile) o;
        return Objects.equals(path, other.path);
    }
}
