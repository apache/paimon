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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/** Checksum validation for serialized deletion vectors. */
class DeletionVectorChecksum {

    static byte[] readAndValidate(DataInputStream in, int bitmapLength, int magicNumber)
            throws IOException {
        CRC32 checksum = new CRC32();
        checksum.update(ByteBuffer.allocate(Integer.BYTES).putInt(magicNumber).array());

        byte[] bitmapData = new byte[bitmapLength - BitmapDeletionVector.MAGIC_NUMBER_SIZE_BYTES];
        in.readFully(bitmapData);
        checksum.update(bitmapData);

        int expectedChecksum = in.readInt();
        int actualChecksum = (int) checksum.getValue();
        if (expectedChecksum != actualChecksum) {
            throw new IOException(
                    "Invalid deletion vector checksum. Expected "
                            + Integer.toUnsignedLong(expectedChecksum)
                            + " but computed "
                            + Integer.toUnsignedLong(actualChecksum)
                            + ".");
        }
        return bitmapData;
    }

    private DeletionVectorChecksum() {}
}
