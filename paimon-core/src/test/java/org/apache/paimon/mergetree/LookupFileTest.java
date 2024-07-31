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

package org.apache.paimon.mergetree;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LookupFile}. */
public class LookupFileTest {

    @Test
    public void testLocalFilePrefix() {
        assertThat(
                        LookupFile.localFilePrefix(
                                "2024073105",
                                10,
                                "data-ccbb95e7-8b8c-4549-8ca9-f553843d67ad-3.orc",
                                100))
                .isEqualTo("2024073105-10-data-ccbb95e7-8b8c-4549-8ca9-f553843d67ad-3.orc");
        assertThat(
                        LookupFile.localFilePrefix(
                                "", 10, "data-ccbb95e7-8b8c-4549-8ca9-f553843d67ad-3.orc", 100))
                .isEqualTo("10-data-ccbb95e7-8b8c-4549-8ca9-f553843d67ad-3.orc");
        assertThat(
                        LookupFile.localFilePrefix(
                                "2024073105",
                                10,
                                "data-ccbb95e7-8b8c-4549-8ca9-f553843d67ad-3.orc",
                                20))
                .isEqualTo("2024073105-10-data-c");
    }
}
