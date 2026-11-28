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

package org.apache.paimon.fs.cache;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.file.AccessDeniedException;

import static org.assertj.core.api.Assertions.assertThat;

class CacheConnectionFailureTest {

    @Test
    void testSocketAndNativeTransportFailures() {
        for (Throwable failure :
                new Throwable[] {
                    new ConnectException("refused"),
                    new SocketTimeoutException("timed out"),
                    new UnknownHostException("cache"),
                    new IOException("SDK", new ConnectException("refused")),
                    new IOException("Failed to connect Http service: http://cache/object"),
                    new IOException(
                            "ErrorCode : 2001, ErrorMessage : open failed, get file internal error:"
                                    + "Failed to connect Http service: http://cache/object")
                }) {
            assertThat(CacheConnectionFailure.isConnectionFailure(failure)).isTrue();
        }
        IOException loader = new IOException("Could not load FileIO");
        loader.addSuppressed(new IOException("SDK", new ConnectException("refused")));
        assertThat(CacheConnectionFailure.isConnectionFailure(loader)).isTrue();
    }

    @Test
    void testLogicalErrorsAndCancellationDoNotTriggerFallback() {
        for (Throwable failure :
                new Throwable[] {
                    new FileNotFoundException("missing"),
                    new AccessDeniedException("denied"),
                    new SecurityException("denied"),
                    new InterruptedIOException("cancelled"),
                    new IOException("file read failed"),
                    new IOException("request timeout"),
                    new IOException("ErrorCode : 133, ErrorMessage : No such file"),
                    new IOException("ErrorCode : 403, ErrorMessage : AccessDenied"),
                    new IOException("SDK", new FileNotFoundException("missing"))
                }) {
            assertThat(CacheConnectionFailure.isConnectionFailure(failure)).isFalse();
        }
        FileNotFoundException missing = new FileNotFoundException("missing");
        missing.addSuppressed(new ConnectException("unrelated close failure"));
        assertThat(CacheConnectionFailure.isConnectionFailure(new IOException(missing))).isFalse();
    }
}
