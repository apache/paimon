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

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSClientBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OSSFileIO}. */
public class OSSFileIOTest {

    /** The swap must replace both operations with the CMK-stamping subclasses. */
    @Test
    public void testSseKmsOperationSwapTakesEffect() throws Exception {
        OSSClient ossClient =
                (OSSClient) new OSSClientBuilder().build("http://oss.example.com", "ak", "sk");
        try {
            OSSFileIO.swapSseKmsOperations(ossClient, "KMS", "my-cmk-key-id");

            assertThat(ossClient.getObjectOperation())
                    .isInstanceOf(OSSFileIO.SseKmsObjectOperation.class);
            assertThat(ossClient.getMultipartOperation())
                    .isInstanceOf(OSSFileIO.SseKmsMultipartOperation.class);
        } finally {
            ossClient.shutdown();
        }
    }
}
