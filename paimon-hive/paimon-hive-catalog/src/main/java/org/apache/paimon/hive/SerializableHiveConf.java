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

package org.apache.paimon.hive;

import io.github.pixee.security.ObjectInputFilters;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Wrap {@link HiveConf} to a serializable class, use lazy serialization and deserialization
 * mechanism to reduce overhead.
 */
public class SerializableHiveConf implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient HiveConf conf;

    private byte[] serializedConf;

    public SerializableHiveConf(HiveConf conf) {
        this.conf = conf;
    }

    public HiveConf conf() {
        if (conf == null && serializedConf != null) {
            deSerializeConf();
        }
        return conf;
    }

    private void deSerializeConf() {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedConf);
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            ObjectInputFilters.enableObjectFilterIfUnprotected(ois);
            this.conf = new HiveConf();
            conf.readFields(ois);
        } catch (IOException e) {
            throw new RuntimeException("Could not deserialize conf", e);
        }
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        if (serializedConf == null && conf != null) {
            serializeConf();
        }
        out.defaultWriteObject();
    }

    private void serializeConf() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            conf.write(oos);
            oos.flush();
            serializedConf = bos.toByteArray();
        } catch (IOException e) {
            throw new IOException("Could not serialize conf", e);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
    }
}
