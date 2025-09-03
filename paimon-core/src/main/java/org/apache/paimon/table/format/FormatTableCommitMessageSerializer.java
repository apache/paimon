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

package org.apache.paimon.table.format;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.format.FormatTableAtomicCommitter.TempFileInfo;
import org.apache.paimon.table.format.FormatTableWrite.FormatTableCommitMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** Serializer for {@link FormatTableCommitMessage} that provides consistent serialization. */
@Public
public class FormatTableCommitMessageSerializer {

    private static final int VERSION = 1;

    /** Serializes a FormatTableCommitMessage to byte array. */
    public byte[] serialize(FormatTableCommitMessage message) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (java.io.DataOutputStream dataOut = new java.io.DataOutputStream(outputStream)) {
            // Write version for future compatibility
            dataOut.writeInt(VERSION);

            // Write partition string
            dataOut.writeUTF(message.getPartition());

            // Write temp file info
            TempFileInfo tempFileInfo = message.getTempFileInfo();
            dataOut.writeUTF(tempFileInfo.getTempPath().toString());
            dataOut.writeUTF(tempFileInfo.getFinalPath().toString());
            dataOut.writeUTF(tempFileInfo.getPartitionPath());

            dataOut.flush();
        }
        return outputStream.toByteArray();
    }

    /** Deserializes a FormatTableCommitMessage from byte array. */
    public FormatTableCommitMessage deserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        try (java.io.DataInputStream dataIn = new java.io.DataInputStream(inputStream)) {
            // Read and validate version
            int version = dataIn.readInt();
            if (version != VERSION) {
                throw new IOException(
                        "Unsupported serialization version: " + version + ", expected: " + VERSION);
            }

            // Read partition string
            String partition = dataIn.readUTF();

            // Read temp file info
            String tempPathStr = dataIn.readUTF();
            String finalPathStr = dataIn.readUTF();
            String partitionPath = dataIn.readUTF();

            TempFileInfo tempFileInfo =
                    new TempFileInfo(new Path(tempPathStr), new Path(finalPathStr), partitionPath);

            return new FormatTableCommitMessage(partition, tempFileInfo);
        }
    }

    /** Gets the version of this serializer. */
    public int getVersion() {
        return VERSION;
    }
}
