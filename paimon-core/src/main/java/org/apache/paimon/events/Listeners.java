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

package org.apache.paimon.events;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentLinkedQueue;

/** Manage subscribers and trigger event notifications. */
@SuppressWarnings({"unchecked", "rawtypes"})
public class Listeners {
    private static final Logger LOG = LoggerFactory.getLogger(Listeners.class);

    private Listeners() {}

    private static final Map<Class<?>, Queue<Listener<?>>> listeners = Maps.newConcurrentMap();

    static {
        Iterator<Listener> iterator =
                ServiceLoader.load(Listener.class, Listener.class.getClassLoader()).iterator();
        iterator.forEachRemaining(listener -> subscribe(listener, listener.eventClazz()));
    }

    public static <E> Queue<Listener<?>> subscribe(Listener<E> listener, Class<E> eventType) {
        Queue<Listener<?>> list =
                listeners.computeIfAbsent(eventType, v -> new ConcurrentLinkedQueue<>());
        list.add(listener);
        return list;
    }

    public static <E> boolean unsubscribe(Listener<E> listener, Class<E> eventType) {
        Queue<Listener<?>> list =
                listeners.computeIfAbsent(eventType, v -> new ConcurrentLinkedQueue<>());
        return list.remove(listener);
    }

    public static <E> void notifyAll(E event) {
        if (event == null) {
            LOG.warn("Event is null. Skip notifying listeners.");
            return;
        }

        Queue<Listener<?>> list = listeners.get(event.getClass());
        if (list != null) {
            for (Listener<?> value : list) {
                Listener<E> listener = (Listener<E>) value;
                listener.notify(event);
            }
        }
    }
}
