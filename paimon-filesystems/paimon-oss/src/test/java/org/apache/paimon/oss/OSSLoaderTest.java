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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OSSLoader}. */
public class OSSLoaderTest {

    /** A credentials provider replaces the access keys, so the OSS plugin is still chosen. */
    @Test
    public void testCredentialsProviderSatisfiesRequiredOptions() throws Exception {
        Options options = new Options();
        options.set("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        options.set(
                "fs.oss.credentials.provider",
                "com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider");

        FileIO fileIO = FileIO.get(new Path("oss://bucket/key"), CatalogContext.create(options));

        assertThat(fileIO.getClass().getEnclosingClass()).isEqualTo(OSSLoader.class);
    }
}
