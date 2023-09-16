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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.Options;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/** Module for encrypting and decrypting table data files, it must be serializable. */
public interface EncryptionManager extends Serializable {

    String identifier();

    void configure(Options options);

    FileIO decrypt(EnvelopeEncryptionManager.EncryptedFileIO encryptedFileIO);

    EnvelopeEncryptionManager.EncryptedFileIO encrypt(FileIO fileIO);

    default Map<String, KmsClient> discoverKmsClient() {
        Iterator<KmsClient> iterator =
                ServiceLoader.load(KmsClient.class, EncryptionManager.class.getClassLoader())
                        .iterator();

        Map<String, KmsClient> map = new HashMap<>();
        while (iterator.hasNext()) {
            KmsClient kmsClient = iterator.next();
            map.put(kmsClient.identifier(), kmsClient);
        }

        return map;
    }
}
