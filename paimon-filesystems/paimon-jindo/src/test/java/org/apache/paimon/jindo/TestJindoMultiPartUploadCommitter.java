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

package org.apache.paimon.jindo;

import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.InstantiationUtil;

import com.aliyun.jindodata.api.spec.protos.JdoObjectPart;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for JindoMultiPartUploadCommitter. */
public class TestJindoMultiPartUploadCommitter {

    @Test
    public void testSerializableJdoObjectPart() throws Exception {
        // Test the SerializableJdoObjectPart wrapper directly
        JdoObjectPart originalPart = new JdoObjectPart();
        originalPart.setPartNum(5);
        originalPart.setSize(2048L);
        originalPart.setETag("test-etag-2");

        SerializableJdoObjectPart wrapper = new SerializableJdoObjectPart(originalPart);

        byte[] serialized = InstantiationUtil.serializeObject(wrapper);
        SerializableJdoObjectPart deserialized =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        assertThat(deserialized).isNotNull();
        assertThat(deserialized.getPartNum()).isEqualTo(5);
        assertThat(deserialized.getSize()).isEqualTo(2048L);
        assertThat(deserialized.getETag()).isEqualTo("test-etag-2");

        JdoObjectPart reconstructedPart = deserialized.toJdoObjectPart();
        assertThat(reconstructedPart.getPartNum()).isEqualTo(5);
        assertThat(reconstructedPart.getSize()).isEqualTo(2048L);
        assertThat(reconstructedPart.getETag()).isEqualTo("test-etag-2");
    }

    @Test
    public void testJindoMultiPartUploadCommitterSerialization() throws Exception {
        String uploadId = "test-upload-id";
        String objectName = "test-object-name";
        long position = 1024L;
        Path targetPath = new Path("/test/path/file.txt");
        List<SerializableJdoObjectPart> uploadedParts = new ArrayList<>();
        JdoObjectPart originalPart = new JdoObjectPart();
        originalPart.setPartNum(5);
        originalPart.setSize(2048L);
        originalPart.setETag("test-etag-2");
        uploadedParts.add(new SerializableJdoObjectPart(originalPart));

        JindoMultiPartUploadCommitter committer =
                new JindoMultiPartUploadCommitter(
                        uploadId, uploadedParts, objectName, position, targetPath);

        byte[] serialized = InstantiationUtil.serializeObject(committer);
        JindoMultiPartUploadCommitter deserialized =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        // Verify the deserialized object
        assertThat(deserialized).isNotNull();
        SerializableJdoObjectPart deserializedPart = deserialized.uploadedParts().get(0);
        assertThat(deserializedPart.getPartNum()).isEqualTo(5);
        assertThat(deserializedPart.getSize()).isEqualTo(2048L);
        assertThat(deserializedPart.getETag()).isEqualTo("test-etag-2");
    }
}
