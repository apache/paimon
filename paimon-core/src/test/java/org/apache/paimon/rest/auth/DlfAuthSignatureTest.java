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

public class DlfAuthSignatureTest {

    @Test
    public void testGetAuthorization() throws Exception {
        RestAuthParameter restAuthParameter =
                new RestAuthParameter(
                        "endpoint",
                        "/v1/catalogs/test/databases/test/tables/test/commit",
                        "POST",
                        "");
        DlfToken token =
                new DlfToken("accessKeyId", "accessKeySecret", "securityToken", "expiration");
        String authorization = DlfAuthSignature.getAuthorization(restAuthParameter, token, "date");
        Assertions.assertEquals(
                DlfAuthSignature.getAuthorization(restAuthParameter, token, "date"), authorization);
    }
}
