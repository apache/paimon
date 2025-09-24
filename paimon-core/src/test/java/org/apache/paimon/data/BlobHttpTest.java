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

package org.apache.paimon.data;

import org.apache.paimon.rest.TestHttpWebServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Blob} http. */
public class BlobHttpTest {

    private TestHttpWebServer server;

    @Before
    public void setUp() throws Exception {
        server = new TestHttpWebServer("/my_path");
        server.start();
    }

    @After
    public void tearDown() throws IOException {
        server.stop();
    }

    @Test
    public void testHttp() {
        String data = "my_data";
        server.enqueueResponse(data, 200);
        Blob blob = Blob.fromHttp(server.getBaseUrl());
        assertThat(blob).isInstanceOf(BlobRef.class);
        assertThat(new String(blob.toData(), UTF_8)).isEqualTo(data);
    }
}
