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
import org.apache.paimon.rest.RESTObjectMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Test for {@link DLFAuthSignature}. */
public class DLFAuthSignatureTest {

    @Test
    public void testGetAuthorization() throws Exception {
        String endpoint = "dlf.cn-hangzhou.aliyuncs.com";
        String region = "cn-hangzhou";
        String dateTime = "20231203T121212Z";
        String date = "20231203";
        Map<String, String> parameters = new HashMap<>();
        parameters.put("k1", "v1");
        parameters.put("k2", "v2");
        String data =
                RESTObjectMapper.OBJECT_MAPPER.writeValueAsString(
                        MockRESTMessage.createDatabaseRequest("database"));
        System.out.println(data);
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter(endpoint, "/v1/paimon/databases", parameters, "POST", data);
        DLFToken token = new DLFToken("access-key-id", "access-key-secret", "securityToken", null);
        Map<String, String> signHeaders =
                DLFAuthProvider.generateSignHeaders(
                        restAuthParameter.host(), restAuthParameter.data(), dateTime);
        String authorization =
                DLFAuthSignature.getAuthorization(
                        restAuthParameter, token, region, signHeaders, date);
        Assertions.assertEquals(
                "DLF4-HMAC-SHA256 Credential=access-key-id/20231203/cn-hangzhou/DlfNext/aliyun_v4_request,AdditionalHeaders=host,Signature=846a6ac1f51bb657e91b3653e7d6e8578bcb4b82495a136a3b9b789b35072967",
                authorization);
    }
}
