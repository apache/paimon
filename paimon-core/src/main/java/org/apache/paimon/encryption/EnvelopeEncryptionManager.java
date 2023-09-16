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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.encryption.kms.KmsClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Map;

/** EnvelopeEncryptionManager. */
public class EnvelopeEncryptionManager implements EncryptionManager {

    private static final Logger LOG = LoggerFactory.getLogger(EnvelopeEncryptionManager.class);

    public static final String IDENTIFIER = "envelope";

    private String tableMasterKey;
    private int dataKeyLength = 16;

    private KmsClient kmsClient;

    private SecureRandom secureRandom = new SecureRandom();

    public EnvelopeEncryptionManager() {}

    public EnvelopeEncryptionManager(String tableMasterKey, int dataKeyLength) {
        this.tableMasterKey = tableMasterKey;
        this.dataKeyLength = dataKeyLength;
    }

    @Override
    public void configure(CoreOptions options) {
        Map<String, KmsClient> map = discoverKmsClient();
        kmsClient = map.get(options.encryptionKmsClient().toString());
        kmsClient.configure(options);
    }

    @Override
    public KeyMetadata encrypt(KmsClient.CreateKeyResult createKeyResult) {
        byte[] key = createKeyResult.key();
        ByteBuffer fileDek = ByteBuffer.allocate(16);
        secureRandom.nextBytes(fileDek.array());
        byte[] plaintextDataKey = fileDek.array();
        byte[] encryptedDataKey = kmsClient.encryptDataKey(plaintextDataKey, key);

        ByteBuffer aadPrefix = ByteBuffer.allocate(16);
        secureRandom.nextBytes(aadPrefix.array());

        return new KeyMetadata(
                createKeyResult.keyId(),
                "aes",
                encryptedDataKey,
                plaintextDataKey,
                aadPrefix.array());
    }

    @Override
    public byte[] decrypt(KeyMetadata keyMetadata) {
        byte[] key = kmsClient.getKey(keyMetadata.keyId());
        return kmsClient.decryptDataKey(keyMetadata.encryptedKey(), key);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
