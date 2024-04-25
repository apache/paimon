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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Provides the ability to bring timeout to blocking iterators. */
public class BlockingIterator<IN, OUT> implements AutoCloseable {

    /**
     * A static cached {@link ExecutorService}. We don't limit the number of threads since the work
     * inside is I/O type.
     */
    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

    private final Iterator<IN> iterator;

    private final Function<IN, OUT> converter;

    public BlockingIterator(Iterator<IN> iterator, Function<IN, OUT> converter) {
        this.iterator = iterator;
        this.converter = converter;
    }

    public static <T> BlockingIterator<T, T> of(Iterator<T> iterator) {
        return new BlockingIterator<>(iterator, t -> t);
    }

    public static <IN, OUT> BlockingIterator<IN, OUT> of(
            Iterator<IN> iterator, Function<IN, OUT> converter) {
        return new BlockingIterator<>(iterator, converter);
    }

    public List<OUT> collectAndClose(int limit) throws Exception {
        try {
            return collect(limit);
        } finally {
            close();
        }
    }

    public List<OUT> collect() throws Exception {
        return collect(Integer.MAX_VALUE);
    }

    public List<OUT> collect(int limit) throws TimeoutException {
        return collect(limit, 3, TimeUnit.MINUTES);
    }

    public List<OUT> collect(int limit, long timeout, TimeUnit unit) throws TimeoutException {
        List<OUT> result = new ArrayList<>();
        Future<List<OUT>> future = EXECUTOR.submit(() -> doCollect(limit, result));
        try {
            return future.get(timeout, unit);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new TimeoutException(
                    String.format(
                            "Cannot collect %s records in %s %s. Currently records: \n %s",
                            limit,
                            timeout,
                            unit,
                            result.stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining("\n"))));
        }
    }

    private List<OUT> doCollect(int limit, List<OUT> result) {
        if (limit == 0) {
            throw new RuntimeException("Collect zero record is meaningless.");
        }

        while (iterator.hasNext()) {
            result.add(converter.apply(iterator.next()));

            if (result.size() == limit) {
                return result;
            }
        }

        if (limit != Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    String.format(
                            "The stream ended before reaching the requested %d records. Only %d records were received. Records list is: \n %s",
                            limit,
                            result.size(),
                            result.stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining("\n"))));
        }

        return result;
    }

    @Override
    public void close() throws Exception {
        if (this.iterator instanceof AutoCloseable) {
            ((AutoCloseable) this.iterator).close();
        }
    }
}
