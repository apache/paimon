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

import java.util.Random;

/** Utils for tests. */
public class RandomUtil {

    private static final Random RANDOM = new Random();

    public static byte[] randomBytes(int length) {
        byte[] b = new byte[length];
        RANDOM.nextBytes(b);
        return b;
    }

    public static String randomString(int length) {
        byte[] buffer = new byte[length];

        for (int i = 0; i < length; i += 1) {
            buffer[i] = (byte) ('a' + RANDOM.nextInt(26));
        }

        return new String(buffer);
    }
}
