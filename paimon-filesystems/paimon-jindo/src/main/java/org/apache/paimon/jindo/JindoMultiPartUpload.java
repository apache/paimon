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

import org.apache.paimon.fs.MultiPartUploadStore;

import com.aliyun.jindodata.api.spec.protos.JdoMpuUploadPartReply;
import com.aliyun.jindodata.api.spec.protos.JdoObjectPart;
import com.aliyun.jindodata.api.spec.protos.JdoObjectPartList;
import com.aliyun.jindodata.common.JindoHadoopSystem;
import com.aliyun.jindodata.store.JindoMpuStore;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/** Provides the multipart upload by Jindo. */
public class JindoMultiPartUpload
        implements MultiPartUploadStore<SerializableJdoObjectPart, String> {

    private final JindoMpuStore mpuStore;
    private final Path workingDirectory;

    public JindoMultiPartUpload(JindoHadoopSystem fs, Path filePath) {
        this.workingDirectory = fs.getWorkingDirectory();
        this.mpuStore = fs.getMpuStore(filePath);
    }

    @Override
    public String pathToObject(Path hadoopPath) {
        return hadoopPath.toString();
    }

    @Override
    public Path workingDirectory() {
        return workingDirectory;
    }

    @Override
    public String startMultiPartUpload(String objectName) throws IOException {
        return mpuStore.initMultiPartUpload(new Path(objectName));
    }

    @Override
    public String completeMultipartUpload(
            String objectName,
            String uploadId,
            List<SerializableJdoObjectPart> partETags,
            long numBytesInParts) {
        try {
            JdoObjectPartList partList =
                    new com.aliyun.jindodata.api.spec.protos.JdoObjectPartList();
            JdoObjectPart[] jdoObjectParts = new JdoObjectPart[partETags.size()];
            for (int i = 0; i < partETags.size(); i++) {
                jdoObjectParts[i] = partETags.get(i).toJdoObjectPart();
            }
            partList.setParts(jdoObjectParts);
            mpuStore.commitMultiPartUpload(new Path(objectName), uploadId, partList);
            return uploadId;
        } catch (Exception e) {
            throw new RuntimeException("Failed to complete multipart upload for: " + objectName, e);
        }
    }

    @Override
    public SerializableJdoObjectPart uploadPart(
            String objectName, String uploadId, int partNumber, File file, int byteLength)
            throws IOException {
        try {
            ByteBuffer buffer;
            try (FileInputStream fis = new FileInputStream(file);
                    FileChannel channel = fis.getChannel()) {
                buffer = ByteBuffer.allocateDirect(byteLength);
                channel.read(buffer);
                buffer.flip();
            }

            JdoMpuUploadPartReply result =
                    mpuStore.uploadPart(new Path(objectName), uploadId, partNumber, buffer);
            return new SerializableJdoObjectPart(result.getPartInfo());
        } catch (Exception e) {
            throw new IOException("Failed to upload part " + partNumber + " for: " + objectName, e);
        }
    }

    @Override
    public void abortMultipartUpload(String objectName, String uploadId) {
        try {
            mpuStore.abortMultipartUpload(new Path(objectName), uploadId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to abort multipart upload for: " + objectName, e);
        }
    }
}
