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

package org.apache.paimon.consumer;

import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.DateTimeUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ConsumerManager}. */
public class ServiceManagerTest {

    @TempDir Path tempDir;

    private ConsumerManager manager;

    @BeforeEach
    public void before() {
        this.manager =
                new ConsumerManager(
                        LocalFileIO.create(), new org.apache.paimon.fs.Path(tempDir.toUri()));
    }

    @Test
    public void test() {
        Optional<Consumer> consumer = manager.consumer("id1");
        assertThat(consumer).isEmpty();

        assertThat(manager.minNextSnapshot()).isEmpty();

        manager.resetConsumer("id1", new Consumer(5));
        consumer = manager.consumer("id1");
        assertThat(consumer).map(Consumer::nextSnapshot).get().isEqualTo(5L);

        manager.resetConsumer("id2", new Consumer(8));
        consumer = manager.consumer("id2");
        assertThat(consumer).map(Consumer::nextSnapshot).get().isEqualTo(8L);

        assertThat(manager.minNextSnapshot()).isEqualTo(OptionalLong.of(5L));
    }

    @Test
    public void testExpire() throws Exception {
        manager.resetConsumer("id1", new Consumer(1));
        Thread.sleep(1000);
        LocalDateTime expireDateTime = DateTimeUtils.toLocalDateTime(System.currentTimeMillis());
        Thread.sleep(1000);
        manager.resetConsumer("id2", new Consumer(2));

        // check expire
        manager.expire(expireDateTime);
        assertThat(manager.consumer("id1")).isEmpty();
        assertThat(manager.consumer("id2")).map(Consumer::nextSnapshot).get().isEqualTo(2L);

        // check last modification
        expireDateTime = DateTimeUtils.toLocalDateTime(System.currentTimeMillis());
        Thread.sleep(1000);
        manager.resetConsumer("id2", new Consumer(3));
        manager.expire(expireDateTime);
        assertThat(manager.consumer("id2")).map(Consumer::nextSnapshot).get().isEqualTo(3L);
    }

    @Test
    public void testReadConsumer() throws Exception {
        manager.resetConsumer("id1", new Consumer(5));
        assertThat(manager.consumer("id1"));
    }
}
