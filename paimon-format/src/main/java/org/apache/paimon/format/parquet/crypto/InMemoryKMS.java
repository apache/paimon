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

package org.apache.paimon.format.parquet.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.KeyToolkit;
import org.apache.parquet.crypto.keytools.KmsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/** In-memory KMS implementation. */
public class InMemoryKMS implements KmsClient {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryKMS.class);

    private static Map<String, byte[]> masterKeyMap;
    private static Map<String, byte[]> newMasterKeyMap;

    public synchronized void initialize(
            Configuration configuration,
            String kmsInstanceID,
            String kmsInstanceURL,
            String accessToken) {
        String[] masterKeys = configuration.getTrimmedStrings("parquet.encryption.key.list");
        if (null != masterKeys && masterKeys.length != 0) {
            masterKeyMap = parseKeyList(masterKeys);
            newMasterKeyMap = masterKeyMap;
        } else {
            throw new ParquetCryptoRuntimeException("No encryption key list");
        }
    }

    private static Map<String, byte[]> parseKeyList(String[] masterKeys) {
        Map<String, byte[]> keyMap = new HashMap<>();

        for (String masterKey : masterKeys) {
            String[] parts = masterKey.split(":");
            String keyName = parts[0].trim();
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "Key '" + keyName + "' is not formatted correctly");
            }

            String key = parts[1].trim();
            try {
                byte[] keyBytes = Base64.getDecoder().decode(key);
                keyMap.put(keyName, keyBytes);
            } catch (IllegalArgumentException e) {
                LOG.warn("Could not decode key '" + keyName + "'!");
                throw e;
            }
        }

        return keyMap;
    }

    public synchronized String wrapKey(byte[] keyBytes, String masterKeyIdentifier)
            throws KeyAccessDeniedException, UnsupportedOperationException {
        byte[] masterKey = newMasterKeyMap.get(masterKeyIdentifier);
        if (null == masterKey) {
            throw new ParquetCryptoRuntimeException("Key not found: " + masterKeyIdentifier);
        } else {
            byte[] add = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
            return KeyToolkit.encryptKeyLocally(keyBytes, masterKey, add);
        }
    }

    public synchronized byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier)
            throws KeyAccessDeniedException, UnsupportedOperationException {
        byte[] masterKey = masterKeyMap.get(masterKeyIdentifier);
        if (null == masterKey) {
            throw new ParquetCryptoRuntimeException("Key not found: " + masterKeyIdentifier);
        } else {
            byte[] add = masterKeyIdentifier.getBytes(StandardCharsets.UTF_8);
            return KeyToolkit.decryptKeyLocally(wrappedKey, masterKey, add);
        }
    }
}
