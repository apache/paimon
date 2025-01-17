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

import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Utility class to make a {@link Configuration Hadoop Configuration} serializable. */
public final class SerializableHadoopConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient Configuration hadoopConfig;

    public SerializableHadoopConfig(Configuration hadoopConfig) {
        this.hadoopConfig = checkNotNull(hadoopConfig);
    }

    public Configuration get() {
        return hadoopConfig;
    }

    // ------------------------------------------------------------------------

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        DataOutputSerializer ser = new DataOutputSerializer(256);
        hadoopConfig.write(ser);
        out.writeInt(ser.length());
        out.write(ser.getSharedBuffer(), 0, ser.length());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        byte[] data = new byte[in.readInt()];
        in.readFully(data);
        DataInputDeserializer deserializer = new DataInputDeserializer(data);
        this.hadoopConfig = new Configuration(false);

        try {
            this.hadoopConfig.readFields(deserializer);
        } catch (IOException e) {
            throw new IOException(
                    "Could not deserialize Hadoop Configuration, the serialized and de-serialized don't match.",
                    e);
        }
    }
}
