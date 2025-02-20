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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Test for {@link DLFAuthSignature}. */
public class DLFAuthSignatureTest {

    @Test
    public void testGetAuthorization() throws Exception {
        String region = "cn-hangzhou";
        String dateTime = "20231203T121212Z";
        String date = "20231203";
        Map<String, String> parameters = new HashMap<>();
        parameters.put("k1", "v1");
        parameters.put("k2", "v2");
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("192.168.0.133", "/v1/config", parameters, "GET", "");
        DLFToken token =
                new DLFToken("access-key-id", "access-key-secret", "securityToken", "expiration");
        Map<String, String> signHeaders =
                DLFAuthProvider.generateSignHeaders(
                        restAuthParameter.host(), restAuthParameter.data(), dateTime);
        String authorization =
                DLFAuthSignature.getAuthorization(
                        restAuthParameter, token, region, signHeaders, date);
        Assertions.assertEquals(
                "DLF4-HMAC-SHA256 Credential=access-key-id/20231203/cn-hangzhou/DlfNext/aliyun_v4_request,AdditionalHeaders=host,Signature=2cd7f160b930543e3fb6a3ebd6c6184aa953a84536d8c4e2e8e108b3215c0128",
                authorization);
    }
}
