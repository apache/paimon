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

import org.apache.paimon.rest.MockRESTMessage;
import org.apache.paimon.rest.RESTApi;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link DLFDefaultSigner}. */
public class DLFAuthSignatureTest {

    @Test
    public void testGetAuthorization() throws Exception {
        String region = "cn-hangzhou";
        String dateTime = "20231203T121212Z";
        String date = "20231203";
        Map<String, String> parameters = new HashMap<>();
        parameters.put("k1", "v1");
        parameters.put("k2", "v2");
        String data = RESTApi.toJson(MockRESTMessage.createDatabaseRequest("database"));
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("/v1/paimon/databases", parameters, "POST", data);
        DLFToken token = new DLFToken("access-key-id", "access-key-secret", "securityToken", null);
        DLFDefaultSigner signer = new DLFDefaultSigner(region);
        Map<String, String> signHeaders =
                signer.signHeaders(
                        data,
                        java.time.Instant.parse("2023-12-03T12:12:12Z"),
                        "securityToken",
                        "host");
        String authorization = signer.authorization(restAuthParameter, token, "host", signHeaders);
        assertEquals(
                "DLF4-HMAC-SHA256 Credential=access-key-id/20231203/cn-hangzhou/DlfNext/aliyun_v4_request,Signature=c72caf1d40b55b1905d891ee3e3de48a2f8bebefa7e39e4f277acc93c269c5e3",
                authorization);
    }
}
