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

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** This class contains reusable utility methods for unit tests. */
public class CommonTestUtils {

    // ------------------------------------------------------------------------
    //  Manipulation of environment
    // ------------------------------------------------------------------------

    public static void setEnv(Map<String, String> newenv) {
        setEnv(newenv, true);
    }

    // This code is taken slightly modified from: http://stackoverflow.com/a/7201825/568695
    // it changes the environment variables of this JVM. Use only for testing purposes!
    @SuppressWarnings("unchecked")
    public static void setEnv(Map<String, String> newenv, boolean clearExisting) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> clazz = env.getClass();
            Field field = clazz.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> map = (Map<String, String>) field.get(env);
            if (clearExisting) {
                map.clear();
            }
            map.putAll(newenv);

            // only for Windows
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            try {
                @SuppressWarnings("JavaReflectionMemberAccess")
                Field theCaseInsensitiveEnvironmentField =
                        processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
                theCaseInsensitiveEnvironmentField.setAccessible(true);
                Map<String, String> cienv =
                        (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
                if (clearExisting) {
                    cienv.clear();
                }
                cienv.putAll(newenv);
            } catch (NoSuchFieldException ignored) {
            }

        } catch (Exception e1) {
            throw new RuntimeException(e1);
        }
    }

    /** Checks whether an exception with a message occurs when running a piece of code. */
    public static void assertThrows(
            String msg, Class<? extends Exception> expected, Callable<?> code) {
        assertThatThrownBy(code::call).isInstanceOf(expected).hasMessageContaining(msg);
    }

    public static void waitUtil(Supplier<Boolean> condition, Duration timeout, Duration pause)
            throws TimeoutException, InterruptedException {
        waitUtil(condition, timeout, pause, "Failed to wait for condition to be true.");
    }

    /**
     * Wait util the given condition is met or timeout.
     *
     * @param condition the condition to wait for.
     * @param timeout the maximum time to wait for the condition to become true.
     * @param pause delay between condition checks.
     * @param errorMsg the error message to include in the <code>TimeoutException</code> if the
     *     condition was not met before timeout.
     * @throws TimeoutException if the condition is not met before timeout.
     * @throws InterruptedException if the thread is interrupted.
     */
    @SuppressWarnings("BusyWait")
    public static void waitUtil(
            Supplier<Boolean> condition, Duration timeout, Duration pause, String errorMsg)
            throws TimeoutException, InterruptedException {
        long timeoutMs = timeout.toMillis();
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("The timeout must be positive.");
        }
        long startingTime = System.currentTimeMillis();
        boolean conditionResult = condition.get();
        while (!conditionResult && System.currentTimeMillis() - startingTime < timeoutMs) {
            conditionResult = condition.get();
            Thread.sleep(pause.toMillis());
        }
        if (!conditionResult) {
            throw new TimeoutException(errorMsg);
        }
    }

    public static int[] generateRandomInts(int count) {
        Random random = new Random(42);

        int[] nums = new int[count];
        for (int i = 0; i < count; i++) {
            nums[i] = random.nextInt(count);
        }
        return nums;
    }
}
