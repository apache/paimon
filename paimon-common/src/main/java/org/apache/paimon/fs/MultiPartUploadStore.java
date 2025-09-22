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

package org.apache.paimon.fs;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.List;

/** MultiPartUploadStore. */
public interface MultiPartUploadStore<T, C> {

    default String pathToObject(Path hadoopPath) {
        if (!hadoopPath.isAbsolute()) {
            hadoopPath = new Path(workingDirectory(), hadoopPath);
        }

        return hadoopPath.toUri().getPath().substring(1);
    }

    Path workingDirectory();

    String startMultiPartUpload(String objectName) throws IOException;

    C completeMultipartUpload(
            String objectName, String uploadId, List<T> partETags, long numBytesInParts)
            throws IOException;

    T uploadPart(String objectName, String uploadId, int partNumber, File file, long byteLength)
            throws IOException;

    void abortMultipartUpload(String objectName, String uploadId) throws IOException;
}
