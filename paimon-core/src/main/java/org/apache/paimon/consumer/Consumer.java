/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.consumer;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Consumer which contains next snapshot.
 */
public class Consumer {

    private static final String FIELD_NEXT_SNAPSHOT = "nextSnapshot";

    private final long nextSnapshot;
    private static final int READ_CONSUMER_RETRY_NUM = 3;
    private static final int READ_CONSUMER_RETRY_INTERVAL = 100;

    @JsonCreator
    public Consumer(@JsonProperty(FIELD_NEXT_SNAPSHOT) long nextSnapshot) {
        this.nextSnapshot = nextSnapshot;
    }

    @JsonGetter(FIELD_NEXT_SNAPSHOT)
    public long nextSnapshot() {
        return nextSnapshot;
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static Consumer fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, Consumer.class);
    }

    public static Optional<Consumer> fromPath(FileIO fileIO, Path path) {
        try {
            if (!fileIO.exists(path)) {
                return Optional.empty();
            }

            int retryNumber = 0;
            Exception exception = null;
            while (retryNumber++ < READ_CONSUMER_RETRY_NUM) {
                try {
                    String json = fileIO.readFileUtf8(path);
                    Optional<Consumer> consumer = Optional.of(Consumer.fromJson(json));
                    exception = null;
                    return consumer;
                } catch (Exception ignored) {
                    exception = ignored;
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(READ_CONSUMER_RETRY_INTERVAL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            if (exception != null) {
                throw new RuntimeException("Fails to read snapshot from path " + path, exception);
            }
        } catch (IOException e) {
            throw new RuntimeException("Fails to read snapshot from path " + path, e);
        }
        return Optional.empty();
    }
}
