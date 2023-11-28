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

package org.apache.paimon.event;

import org.apache.paimon.events.Listener;
import org.apache.paimon.events.Listeners;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Queue;

public class TestListeners {

    @Test
    public void testListeners() {
        TestListener second = new TestListener();
        boolean isRemoved = Listeners.unsubscribe(second, MyEvent.class);
        Assertions.assertThat(isRemoved).isFalse();

        Queue<Listener<?>> listeners = Listeners.subscribe(second, MyEvent.class);
        Assertions.assertThat(listeners.size()).isEqualTo(2);

        MyEvent myEvent = new MyEvent();
        Assertions.assertThat(myEvent.getValue()).isEqualTo(0);

        Listeners.notifyAll(myEvent);

        Assertions.assertThat(myEvent.getValue()).isEqualTo(2);

        isRemoved = Listeners.unsubscribe(second, MyEvent.class);
        Assertions.assertThat(isRemoved).isTrue();
        Listeners.notifyAll(myEvent);
        Assertions.assertThat(myEvent.getValue()).isEqualTo(0 + 2 + 1);
    }
}
