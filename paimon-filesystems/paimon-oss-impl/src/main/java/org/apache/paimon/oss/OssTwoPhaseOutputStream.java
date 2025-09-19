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

package org.apache.paimon.oss;

import org.apache.paimon.fs.MultiPartUploadStore;
import org.apache.paimon.fs.MultiPartUploadTwoPhaseOutputStream;

import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.PartETag;

import java.io.IOException;

/** OSS implementation of TwoPhaseOutputStream using multipart upload. */
public class OssTwoPhaseOutputStream
        extends MultiPartUploadTwoPhaseOutputStream<PartETag, CompleteMultipartUploadResult> {

    public OssTwoPhaseOutputStream(
            MultiPartUploadStore<PartETag, CompleteMultipartUploadResult> multiPartUploadStore,
            org.apache.hadoop.fs.Path hadoopPath)
            throws IOException {
        super(multiPartUploadStore, hadoopPath);
    }

    @Override
    public long partSizeThreshold() {
        return 10L << 20;
    }
}
