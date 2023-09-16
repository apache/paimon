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

package org.apache.paimon.utils;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.GeneralSecurityException;

/** The encryption utils. */
public class EncryptionUtils {

    private static final int NONCE_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 16;
    private static final int GCM_TAG_LENGTH_BITS = 8 * GCM_TAG_LENGTH;

    public static byte[] encryptKeyLocally(byte[] plaintext, byte[] masterKey) {
        AesGcmEncryptor aesGcmEncryptor = new AesGcmEncryptor(masterKey);
        return aesGcmEncryptor.encrypt(plaintext);
    }

    public static byte[] decryptKeyLocally(byte[] encryptedKey, byte[] masterKey) {
        AesGcmDecryptor aesGcmDecryptor = new AesGcmDecryptor(masterKey);
        return aesGcmDecryptor.encrypt(encryptedKey);
    }

    private static class AesGcmEncryptor {

        private final Cipher cipher;

        public AesGcmEncryptor(byte[] masterKey) {
            try {
                cipher = Cipher.getInstance("AES/GCM/NoPadding");
                SecretKeySpec secretKeySpec = new SecretKeySpec(masterKey, "AES");
                byte[] nonce = new byte[NONCE_LENGTH];
                GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
                cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, spec);
            } catch (GeneralSecurityException e) {
                throw new RuntimeException(e);
            }
        }

        public byte[] encrypt(byte[] plaintext) {
            try {
                return cipher.doFinal(plaintext);
            } catch (IllegalBlockSizeException | BadPaddingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class AesGcmDecryptor {

        private final Cipher cipher;

        public AesGcmDecryptor(byte[] masterKey) {
            try {
                cipher = Cipher.getInstance("AES/GCM/NoPadding");
                SecretKeySpec secretKeySpec = new SecretKeySpec(masterKey, "AES");
                byte[] nonce = new byte[NONCE_LENGTH];
                GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
                cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, spec);
            } catch (GeneralSecurityException e) {
                throw new RuntimeException(e);
            }
        }

        public byte[] encrypt(byte[] plaintext) {
            try {
                return cipher.doFinal(plaintext);
            } catch (IllegalBlockSizeException | BadPaddingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
