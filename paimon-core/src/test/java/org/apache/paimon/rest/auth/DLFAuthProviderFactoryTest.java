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

package org.apache.paimon.rest.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link DLFAuthProviderFactory}. */
class DLFAuthProviderFactoryTest {

    @Test
    void getRegion() {
        String region = "cn-hangzhou";
        String url = "https://dlf-" + region + "-internal.aliyuncs.com";
        assertEquals(region, DLFAuthProviderFactory.parseRegionFromUri(url));
        url = "https://dlf-" + region + ".aliyuncs.com";
        assertEquals(region, DLFAuthProviderFactory.parseRegionFromUri(url));
        url = "https://dlf-pre-" + region + ".aliyuncs.com";
        assertEquals(region, DLFAuthProviderFactory.parseRegionFromUri(url));
        region = "us-east-1";
        url = "https://dlf-" + region + ".aliyuncs.com";
        assertEquals(region, DLFAuthProviderFactory.parseRegionFromUri(url));
        url = "https://dlf-" + region + "-internal.aliyuncs.com";
        assertEquals(region, DLFAuthProviderFactory.parseRegionFromUri(url));
        String ipPortUri = "http://127.0.0.1:8080";
        assertThrows(
                IllegalArgumentException.class,
                () -> DLFAuthProviderFactory.parseRegionFromUri(ipPortUri));
    }
}
