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

package org.apache.paimon.consumer;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.exc.MismatchedInputException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

/** Consumer which contains next snapshot. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Consumer {

    private static final String FIELD_NEXT_SNAPSHOT = "nextSnapshot";

    private final long nextSnapshot;

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
        int retryNumber = 0;
        MismatchedInputException exception = null;
        while (retryNumber++ < 5) {
            try {
                return fileIO.readOverwrittenFileUtf8(path).map(Consumer::fromJson);
            } catch (MismatchedInputException e) {
                // retry
                exception = e;
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        throw new UncheckedIOException(exception);
    }
}
