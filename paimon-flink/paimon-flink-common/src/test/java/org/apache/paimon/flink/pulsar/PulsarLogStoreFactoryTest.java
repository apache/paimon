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

package org.apache.paimon.flink.pulsar;

import org.apache.paimon.flink.pulsar.PulsarLogStoreFactory;
import org.junit.Test;

/**
 * Utils for the test of {@link PulsarLogStoreFactory}.
 */
public class PulsarLogStoreFactoryTest {

    @Test
    public void testKeyStyleWapper() {
        assert PulsarLogStoreFactory.changeKeyStyle("pulsar.topic").equals("pulsar.topic");
        assert PulsarLogStoreFactory.changeKeyStyle("pulsar.admin.admin-url").equals("pulsar.admin.adminUrl");
        assert PulsarLogStoreFactory.changeKeyStyle("pulsar.client.service-url").equals("pulsar.client.serviceUrl");
    }
}
