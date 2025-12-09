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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.function.Function;

/** Utils for retry operations. */
public class RetryUtils {

    public static <I, R> Optional<R> retry(
            SupplierWithIOException<I> inputSupplier, Function<I, R> retryFunc) {
        int retryNumber = 0;
        Exception exception = null;
        while (retryNumber++ < 10) {
            I input;
            try {
                input = inputSupplier.get();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (input == null) {
                return Optional.empty();
            }
            try {
                return Optional.ofNullable(retryFunc.apply(input));
            } catch (Exception e) {
                // retry
                exception = e;
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
        throw new RuntimeException("Retry fail after 10 times.", exception);
    }
}
