/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.paimon.encryption;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** Wrap information about the key. */
public class KeyMetadata {

    private final String keyId;
    private final String algorithm;
    private final byte[] encryptedKey;
    private final byte[] plaintextKey;
    private final byte[] aadPrefix;

    public KeyMetadata(
            String keyId,
            String algorithm,
            byte[] encryptedKey,
            byte[] plaintextKey,
            byte[] aadPrefix) {
        this.keyId = keyId;
        this.algorithm = algorithm;
        this.encryptedKey = encryptedKey;
        this.plaintextKey = plaintextKey;
        this.aadPrefix = aadPrefix;
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_KEY_ID", newStringType(false)));
        fields.add(new DataField(1, "_ALGORITHM", newStringType(false)));
        fields.add(new DataField(2, "_ENCRYPTED_KEY", newBytesType(false)));
        fields.add(new DataField(3, "_AAD_PREFIX", newBytesType(true)));
        return new RowType(fields);
    }

    public byte[] encryptedKey() {
        return encryptedKey;
    }

    public byte[] plaintextKey() {
        return plaintextKey;
    }

    public byte[] aadPrefix() {
        return aadPrefix;
    }

    public String keyId() {
        return keyId;
    }

    public String algorithm() {
        return algorithm;
    }
}
