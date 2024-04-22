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

import java.io.IOException;
import java.util.Objects;

/** Index file for data file. */
public class IndexFile {

    private final String path;

    public IndexFile(String path) {
        this.path = path;
    }

    public void serialize(DataOutputView out) throws IOException {
        if (path != null) {
            out.writeBoolean(true);
            out.writeUTF(path);
        } else {
            out.writeBoolean(false);
        }
    }

    public static IndexFile deserialize(DataInputView in) throws IOException {
        return in.readBoolean() ? new IndexFile(in.readUTF()) : new IndexFile(null);
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
