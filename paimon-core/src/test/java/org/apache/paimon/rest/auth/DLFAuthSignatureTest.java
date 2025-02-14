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

/** Test for {@link DLFAuthSignature}. */
public class DLFAuthSignatureTest {

    @Test
    public void testGetAuthorization() throws Exception {
        String date = DLFAuthProvider.getDate();
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter(
                        "endpoint",
                        "/v1/catalogs/test/databases/test/tables/test/commit",
                        "POST",
                        "");
        DLFToken token =
                new DLFToken("accessKeyId", "accessKeySecret", "securityToken", "expiration");
        String dataMd5Hex = DLFAuthSignature.md5Hex(restAuthParameter.data());
        String authorization =
                DLFAuthSignature.getAuthorization(restAuthParameter, token, dataMd5Hex, date);
        Assertions.assertEquals(
                DLFAuthSignature.getAuthorization(restAuthParameter, token, dataMd5Hex, date),
                authorization);
    }
}
