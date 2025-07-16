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

package org.apache.paimon.flink.clone.files;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;

import java.io.Serializable;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Data File (table, partition) with necessary information. */
public class DataFileInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Identifier identifier;
    private final byte[] partition;
    private final byte[] dataFileMeta;

    public DataFileInfo(Identifier identifier, BinaryRow partition, byte[] dataFileMeta) {
        this.identifier = identifier;
        this.partition = serializeBinaryRow(partition);
        this.dataFileMeta = dataFileMeta;
    }

    public Identifier identifier() {
        return identifier;
    }

    public BinaryRow partition() {
        return deserializeBinaryRow(partition);
    }

    public byte[] dataFileMeta() {
        return dataFileMeta;
    }
}
