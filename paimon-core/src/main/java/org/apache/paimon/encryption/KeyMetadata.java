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

package org.apache.paimon.encryption;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.SerializationUtils.newBytesType;

/** Wrap information about the key. */
public class KeyMetadata {

    private final byte[] kekID; // kek id, used to cache kek
    private final byte[] wrappedKEK; // encrypted kek
    private final byte[] wrappedDEK; // encrypted data key
    private final byte[] aadPrefix; // aad prefix, only used for parquet format

    private KeyMetadata() {
        this(new byte[0], new byte[0], new byte[0], new byte[0]);
    }

    public KeyMetadata(byte[] kekID, byte[] wrappedKEK, byte[] wrappedDEK, byte[] aadPrefix) {
        this.kekID = kekID;
        this.wrappedKEK = wrappedKEK;
        this.wrappedDEK = wrappedDEK;
        this.aadPrefix = aadPrefix;
    }

    public static KeyMetadata emptyKeyMetadata() {
        return new KeyMetadata();
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(1, "_KEK_ID", newBytesType(false)));
        fields.add(new DataField(1, "_WRAPPED_KEK", newBytesType(false)));
        fields.add(new DataField(1, "_WRAPPED_DEK", newBytesType(false)));
        fields.add(new DataField(2, "_AAD_PREFIX", newBytesType(false)));
        return new RowType(fields);
    }

    public byte[] aadPrefix() {
        return aadPrefix;
    }

    public byte[] kekID() {
        return kekID;
    }

    public byte[] wrappedKEK() {
        return wrappedKEK;
    }

    public byte[] wrappedDEK() {
        return wrappedDEK;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KeyMetadata)) {
            return false;
        }
        KeyMetadata that = (KeyMetadata) o;
        return Arrays.equals(kekID, that.kekID)
                && Arrays.equals(wrappedKEK, that.wrappedKEK)
                && Arrays.equals(wrappedDEK, that.wrappedDEK)
                && Arrays.equals(aadPrefix, that.aadPrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(kekID),
                Arrays.hashCode(wrappedKEK),
                Arrays.hashCode(wrappedDEK),
                Arrays.hashCode(aadPrefix));
    }
}
