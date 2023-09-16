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

import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.EncryptionUtils;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

/** Encrypt data file using envelope encryption mechanism. */
public class EnvelopeEncryptionManager implements EncryptionManager {

    private static final String IDENTIFIER = "envelope";
    private static final int KEY_ENCRYPTION_KEY_LENGTH = 16;
    private static final SecureRandom RANDOM = new SecureRandom();
    // kekId -> kek
    private static final Map<byte[], KeyEncryptionKey> KEY_MAP = new HashMap<>();
    private static KmsClient kmsClient;

    @Override
    public void configure(Options options) {
        Map<String, KmsClient> map = discoverKmsClient();
        kmsClient = map.get(options.get(CatalogOptions.ENCRYPTION_MECHANISM.key()));
        kmsClient.configure(options);
    }

    @Override
    public FileIO decrypt(EncryptedFileIO encryptedFileIO) {
        return encryptedFileIO.fileIO();
    }

    @Override
    public EncryptedFileIO encrypt(FileIO fileIO) {
        return new EncryptedFileIO(fileIO);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /** Wrapped encrypted FileIO. */
    public static class EncryptedFileIO {
        private final FileIO fileIO;
        private KeyMetadata keyMetadata;
        private FileFormatFactory.FormatContext formatContext;

        public EncryptedFileIO(FileIO fileIO) {
            this.fileIO = fileIO;
        }

        public FileIO fileIO() {
            return fileIO;
        }

        /**
         * when read file, we load the keyMetadata from manifest file, and set it.
         *
         * @param keyMetadata the key metadata
         */
        public void keyMetadata(KeyMetadata keyMetadata) {
            this.keyMetadata = keyMetadata;
        }

        /**
         * when write the KeyMetadata to manifest file, we generate the KeyMetadata from
         * FormatContext.
         *
         * @return the KeyMetadata
         */
        public KeyMetadata keyMetadata() {
            if (null == keyMetadata) {

                if (formatContext != null && formatContext.plaintextDataKey() != null) {
                    byte[] kekId = formatContext.dataAADPrefix();

                    byte[] kekBytes = new byte[KEY_ENCRYPTION_KEY_LENGTH];
                    RANDOM.nextBytes(kekBytes);

                    byte[] wrappedDEK =
                            EncryptionUtils.encryptKeyLocally(
                                    formatContext.plaintextDataKey(), kekBytes);

                    KeyEncryptionKey kek =
                            KEY_MAP.computeIfAbsent(
                                    kekId,
                                    bytes -> {
                                        byte[] wrappedKEK =
                                                kmsClient.wrapKey(
                                                        kekBytes,
                                                        formatContext.encryptionTableId());
                                        return new KeyEncryptionKey(kekBytes, wrappedKEK);
                                    });

                    this.keyMetadata =
                            new KeyMetadata(
                                    kekId, kek.kek(), wrappedDEK, formatContext.dataAADPrefix());
                }
            }
            return keyMetadata;
        }

        public void formatContext(FileFormatFactory.FormatContext formatContext) {
            this.formatContext = formatContext;
        }

        public FileFormatFactory.FormatContext formatContext() {
            // when read file, decrypt the plaintext data key from keyMetadata.
            if (null != keyMetadata && null == formatContext.plaintextDataKey()) {
                KeyEncryptionKey kek =
                        KEY_MAP.computeIfAbsent(
                                keyMetadata.kekID(),
                                kekIdentifier -> {
                                    byte[] kekBytes =
                                            kmsClient.unwrapKey(
                                                    keyMetadata.wrappedKEK(),
                                                    formatContext.encryptionTableId());
                                    return new KeyEncryptionKey(kekBytes, keyMetadata.wrappedKEK());
                                });

                byte[] unWrappedDEK =
                        EncryptionUtils.decryptKeyLocally(keyMetadata.wrappedDEK(), kek.kek());
                formatContext = formatContext.newDataKey(unWrappedDEK);
            }

            return formatContext;
        }
    }

    static class KeyEncryptionKey {
        private final byte[] kek;
        private final byte[] wrappedKEK;

        public KeyEncryptionKey(byte[] kek, byte[] wrappedKEK) {
            this.kek = kek;
            this.wrappedKEK = wrappedKEK;
        }

        public byte[] kek() {
            return kek;
        }
    }
}
