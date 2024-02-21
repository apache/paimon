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

package org.apache.paimon.service.messages;

import org.apache.paimon.data.BinaryRow;

import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBuf;
import org.apache.paimon.shade.netty4.io.netty.buffer.UnpooledByteBufAllocator;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.apache.paimon.service.messages.KvRequestTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KvResponse}. */
public class KvResponseTest {

    @Test
    void testSerialization() {
        KvResponse request = random();
        byte[] serialize = request.serialize();
        ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.heapBuffer();
        byteBuf.writeBytes(serialize);
        KvResponse newRequest = new KvResponse.KvResponseDeserializer().deserializeMessage(byteBuf);
        assertThat(newRequest).isEqualTo(request);
    }

    public static KvResponse random() {
        Random rnd = new Random();
        BinaryRow[] values = new BinaryRow[rnd.nextInt(100)];
        for (int i = 0; i < values.length; i++) {
            if (rnd.nextInt(10) != 0) {
                values[i] = row(rnd.nextInt());
            }
        }
        return new KvResponse(values);
    }
}
