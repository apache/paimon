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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.SerializationUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** A simple {@link FileEntry} only contains identifier and partition. */
public class PartitionFileEntry implements FileEntry {
    private final FileKind kind;
    private final BinaryRow partition;

    public PartitionFileEntry(FileKind kind, BinaryRow partition) {
        this.kind = kind;
        this.partition = partition;
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList();
        fields.add(new DataField(0, "_KIND", new TinyIntType(false)));
        fields.add(new DataField(1, "_PARTITION", SerializationUtils.newBytesType(false)));
        return new RowType(fields);
    }

    public FileKind kind() {
        return this.kind;
    }

    public BinaryRow partition() {
        return this.partition;
    }

    public int bucket() {
        return 0;
    }

    @Override
    public int level() {
        return 0;
    }

    @Override
    public String fileName() {
        return null;
    }

    public Identifier identifier() {
        return new Identifier(partition, 0, 0, UUID.randomUUID().toString());
    }

    @Override
    public BinaryRow minKey() {
        return null;
    }

    @Override
    public BinaryRow maxKey() {
        return null;
    }
}
