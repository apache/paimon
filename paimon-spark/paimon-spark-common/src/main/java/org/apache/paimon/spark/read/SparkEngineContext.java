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

package org.apache.paimon.spark.read;

import org.apache.paimon.utils.SerializableFunction;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Tiny wrapper around the active {@link SparkSession} that exposes RDD style {@code map} / {@code
 * flatMap} primitives over a Java {@link List}. Used by Paimon-on-Spark to dispatch
 * embarrassingly-parallel work (e.g. per-split vector search) to the cluster without forcing the
 * caller to depend on Spark types directly.
 */
public class SparkEngineContext {

    private final JavaSparkContext jsc;

    public SparkEngineContext() {
        this.jsc = JavaSparkContext.fromSparkContext(SparkSession.active().sparkContext());
    }

    public <T> Broadcast<T> broadcast(T value) {
        return jsc.broadcast(value);
    }

    public <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism) {
        if (data.isEmpty()) {
            return Collections.emptyList();
        }
        return jsc.parallelize(data, parallelism).map(func::apply).collect();
    }

    public <I, O> List<O> flatMap(
            List<I> data, SerializableFunction<I, Stream<O>> func, int parallelism) {
        if (data.isEmpty()) {
            return Collections.emptyList();
        }
        return jsc.parallelize(data, parallelism).flatMap(x -> func.apply(x).iterator()).collect();
    }
}
