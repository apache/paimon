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

import org.apache.paimon.options.Options;
import org.apache.paimon.utils.EncryptionUtils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** This is only for test, not for production. */
public class MemoryKMS implements KmsClient {

    private static final String IDENTIFIER = "memory";

    private final Map<String, byte[]> keyMap = new ConcurrentHashMap<>();

    @Override
    public void configure(Options options) {}

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public byte[] wrapKey(byte[] dataKey, String masterKeyId) {
        byte[] masterKey = keyMap.get(masterKeyId);
        if (null == masterKey) {
            throw new RuntimeException("Key not found: " + masterKeyId);
        }
        return EncryptionUtils.encryptKeyLocally(dataKey, masterKey);
    }

    @Override
    public byte[] unwrapKey(byte[] wrappedKey, String masterKeyId) {
        byte[] masterKey = keyMap.get(masterKeyId);
        if (null == masterKey) {
            throw new RuntimeException("Key not found: " + masterKeyId);
        }
        return EncryptionUtils.decryptKeyLocally(wrappedKey, masterKey);
    }

    @Override
    public void close() throws IOException {
        keyMap.clear();
    }
}
