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

package org.apache.paimon.s3;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link S3Loader}. */
public class S3LoaderTest {

    @Test
    public void testScheme() {
        assertThat(new S3Loader().getScheme()).isEqualTo("s3");
    }

    /**
     * The S3 loader must not declare any required options. Access key / secret key are optional so
     * that credential-less authentication (IAM instance profile, environment variables, ...) routes
     * through the bundled S3 plugin instead of being gated out and falling back to another {@code
     * FileIO}.
     */
    @Test
    public void testRequiresNoOptions() {
        assertThat(new S3Loader().requiredOptions()).isEmpty();
    }
}
