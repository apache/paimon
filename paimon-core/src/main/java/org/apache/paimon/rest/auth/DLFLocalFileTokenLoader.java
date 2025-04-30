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

import org.apache.paimon.utils.FileIOUtils;

import java.io.File;

import static org.apache.paimon.rest.RESTObjectMapper.OBJECT_MAPPER;

/** DLF Token Loader for local file. */
public class DLFLocalFileTokenLoader implements DLFTokenLoader {

    private final String tokenFilePath;

    public DLFLocalFileTokenLoader(String tokenFilePath) {
        this.tokenFilePath = tokenFilePath;
    }

    @Override
    public DLFToken loadToken() {
        return readToken(tokenFilePath);
    }

    @Override
    public String description() {
        return tokenFilePath;
    }

    protected static DLFToken readToken(String tokenFilePath) {
        int retry = 1;
        Exception lastException = null;
        while (retry <= 5) {
            try {
                String tokenStr = FileIOUtils.readFileUtf8(new File(tokenFilePath));
                return OBJECT_MAPPER.readValue(tokenStr, DLFToken.class);
            } catch (Exception e) {
                lastException = e;
            }
            try {
                Thread.sleep(retry * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            retry++;
        }
        throw new RuntimeException(lastException);
    }
}
