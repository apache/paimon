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

package org.apache.paimon.encryption.kms;

import org.apache.paimon.CoreOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/** HadoopKms. */
public class HadoopKms extends KmsClientBase {

    public static final String IDENTIFIER = "hadoop";

    private KeyProvider keyProvider;

    private KeyProvider.Options keyOptions;

    @Override
    public void configure(CoreOptions options) {
        Configuration hadoopConf = new Configuration();
        try {
            keyProvider = createProvider(new URI(options.kmsHadoopURI()), hadoopConf);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
        keyOptions = new KeyProvider.Options(hadoopConf);
        keyOptions.setCipher("AES/CTR/NoPadding");
        keyOptions.setBitLength(128);
        keyOptions.setDescription("xxxxx");
    }

    private KeyProvider createProvider(URI uri, Configuration conf) throws IOException {
        final KeyProvider ret;
        try {
            ret = new KMSClientProvider(uri, conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    @Override
    public CreateKeyResult createKey() {
        try {
            String keyId = UUID.randomUUID().toString();
            KeyProvider.KeyVersion kv0 = keyProvider.createKey(keyId, keyOptions);
            return new CreateKeyResult(keyId, kv0.getMaterial());
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getKey(String keyId) {
        try {
            return keyProvider.getCurrentKey(keyId).getMaterial();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public void close() throws IOException {}
}
