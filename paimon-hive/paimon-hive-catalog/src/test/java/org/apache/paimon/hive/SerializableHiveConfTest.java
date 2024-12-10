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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SerializableHiveConf}. */
public class SerializableHiveConfTest {
    @Test
    public void testSerializeHiveConf() throws IOException, ClassNotFoundException {
        HiveConf conf = new HiveConf();
        conf.set("k1", "v1");
        conf.set("k2", "v2");
        int size = conf.size();
        SerializableHiveConf serializableHiveConf = new SerializableHiveConf(conf);

        // serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(serializableHiveConf);
        oos.close();

        // deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        ObjectInputFilters.enableObjectFilterIfUnprotected(ois);
        SerializableHiveConf deserializedHiveConf = (SerializableHiveConf) ois.readObject();
        ois.close();

        HiveConf deserializedConf = deserializedHiveConf.conf();
        assertThat(deserializedConf.size()).isEqualTo(size);
        assertThat(deserializedConf.get("k1")).isEqualTo("v1");
        assertThat(deserializedConf.get("k2")).isEqualTo("v2");
    }
}
