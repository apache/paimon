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

import org.apache.paimon.fs.BaseMultiPartUploadCommitter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.MultiPartUploadStore;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.Pair;

import com.aliyun.jindodata.common.JindoHadoopSystem;

import java.io.IOException;
import java.util.List;

/** Jindo implementation of MultiPartUploadCommitter. */
public class JindoMultiPartUploadCommitter
        extends BaseMultiPartUploadCommitter<SerializableJdoObjectPart, String> {

    public JindoMultiPartUploadCommitter(
            String uploadId,
            List<SerializableJdoObjectPart> uploadedParts,
            String objectName,
            long position,
            Path targetPath) {
        super(uploadId, uploadedParts, objectName, position, targetPath);
    }

    @Override
    protected MultiPartUploadStore<SerializableJdoObjectPart, String> multiPartUploadStore(
            FileIO fileIO, Path targetPath) throws IOException {
        JindoFileIO jindoFileIO = (JindoFileIO) fileIO;
        org.apache.hadoop.fs.Path hadoopPath = jindoFileIO.path(targetPath);
        Pair<JindoHadoopSystem, String> pair = jindoFileIO.getFileSystemPair(hadoopPath);
        JindoHadoopSystem fs = pair.getKey();
        return new JindoMultiPartUpload(fs, hadoopPath);
    }
}
