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

package org.apache.paimon.utils;

import org.slf4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.Arrays;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Utils for thread. */
public class ThreadUtils {

    public static String currentStackString() {
        StackTraceElement[] trace = Thread.getAllStackTraces().get(Thread.currentThread());
        StringBuilder builder = new StringBuilder();
        for (StackTraceElement traceElement : trace) {
            builder.append("\nat ").append(traceElement);
        }
        return builder.toString();
    }

    public static void errorLogThreadDump(Logger logger) {
        final ThreadInfo[] perThreadInfo =
                ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
        logger.error(
                "Thread dump: \n{}",
                Arrays.stream(perThreadInfo).map(Object::toString).collect(Collectors.joining()));
    }

    public static boolean stackContains(String name) {
        StackTraceElement[] ss = new RuntimeException().getStackTrace();
        for (StackTraceElement s : ss) {
            if (s.toString().contains(name)) {
                return true;
            }
        }
        return false;
    }

    public static ThreadFactory newDaemonThreadFactory(final String prefix) {
        final ThreadFactory namedFactory = getNamedThreadFactory(prefix);
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = namedFactory.newThread(r);
                if (!t.isDaemon()) {
                    t.setDaemon(true);
                }
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
            }
        };
    }

    private static ThreadFactory getNamedThreadFactory(final String prefix) {
        SecurityManager s = System.getSecurityManager();
        final ThreadGroup threadGroup =
                (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();

        return new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            private final ThreadGroup group = threadGroup;

            @Override
            public Thread newThread(Runnable r) {
                final String name = prefix + "-t" + threadNumber.getAndIncrement();
                return new Thread(group, r, name);
            }
        };
    }
}
