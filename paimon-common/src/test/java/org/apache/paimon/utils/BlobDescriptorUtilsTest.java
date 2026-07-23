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

import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.Path;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobDescriptorUtils}. */
class BlobDescriptorUtilsTest {

    @Test
    void testValidateTableRoot() {
        Path tableRoot = new Path("oss://bucket/table");

        assertThatCode(
                        () ->
                                BlobDescriptorUtils.validateTableRoot(
                                        tableRoot,
                                        new BlobDescriptor(
                                                "oss://bucket/table/bucket-0/data.blob", 0, 1)))
                .doesNotThrowAnyException();

        assertInvalid(tableRoot, "oss://bucket/table-copy/data.blob");
        assertInvalid(tableRoot, "oss://bucket/table/../other/data.blob");
        assertInvalid(tableRoot, "oss://other/table/data.blob");
        assertInvalid(tableRoot, "s3://bucket/table/data.blob");
    }

    private static void assertInvalid(Path tableRoot, String uri) {
        assertThatThrownBy(
                        () ->
                                BlobDescriptorUtils.validateTableRoot(
                                        tableRoot, new BlobDescriptor(uri, 0, 1)))
                .isInstanceOf(IOException.class);
    }
}
