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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.paimon.rest.RESTObjectMapper.OBJECT_MAPPER;

/** DLF Token Loader for local file. */
public class DLFLocalFileTokenLoader implements DLFTokenLoader {

    private static final long[] READ_TOKEN_FILE_BACKOFF_WAIT_TIME_MILLIS = {1_000, 3_000, 5_000};

    private final String tokenFilePath;

    public DLFLocalFileTokenLoader(String tokenFilePath) {
        this.tokenFilePath = tokenFilePath;
    }

    @Override
    public DLFToken loadToken() {
        return readToken(tokenFilePath, 0);
    }

    protected static DLFToken readToken(String tokenFilePath, int retryTimes) {
        try {
            File tokenFile = new File(tokenFilePath);
            if (tokenFile.exists()) {
                String tokenStr = FileIOUtils.readFileUtf8(tokenFile);
                return OBJECT_MAPPER.readValue(tokenStr, DLFToken.class);
            } else if (retryTimes < READ_TOKEN_FILE_BACKOFF_WAIT_TIME_MILLIS.length - 1) {
                Thread.sleep(READ_TOKEN_FILE_BACKOFF_WAIT_TIME_MILLIS[retryTimes]);
                return readToken(tokenFilePath, retryTimes + 1);
            } else {
                throw new FileNotFoundException(tokenFilePath);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
