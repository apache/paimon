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

package org.apache.paimon.flink;

import org.apache.paimon.flink.sink.FixedBucketSink;
import org.apache.paimon.flink.source.ContinuousFileStoreSource;
import org.apache.paimon.flink.source.StaticFileStoreSource;

import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * ITCase for {@link StaticFileStoreSource}, {@link ContinuousFileStoreSource} and {@link
 * FixedBucketSink}.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class FileStoreWithBranchITCase extends FileStoreITCase {
    public FileStoreWithBranchITCase(boolean isBatch) {
        super(isBatch);
    }

    @BeforeAll
    public static void before() {
        branch = "testBranch";
    }
}
