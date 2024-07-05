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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;

/** Json represents a success file. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SuccessFile {

    private static final String FIELD_CREATION_TIME = "creationTime";
    private static final String FIELD_MODIFICATION_TIME = "modificationTime";

    @JsonProperty(FIELD_CREATION_TIME)
    private final long creationTime;

    @JsonProperty(FIELD_MODIFICATION_TIME)
    private final long modificationTime;

    @JsonCreator
    public SuccessFile(
            @JsonProperty(FIELD_CREATION_TIME) long creationTime,
            @JsonProperty(FIELD_MODIFICATION_TIME) long modificationTime) {
        this.creationTime = creationTime;
        this.modificationTime = modificationTime;
    }

    @JsonGetter(FIELD_CREATION_TIME)
    public long creationTime() {
        return creationTime;
    }

    @JsonGetter(FIELD_MODIFICATION_TIME)
    public long modificationTime() {
        return modificationTime;
    }

    public SuccessFile updateModificationTime(long modificationTime) {
        return new SuccessFile(creationTime, modificationTime);
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static SuccessFile fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, SuccessFile.class);
    }

    @Nullable
    public static SuccessFile safelyFromPath(FileIO fileIO, Path path) throws IOException {
        try {
            String json = fileIO.readFileUtf8(path);
            return SuccessFile.fromJson(json);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SuccessFile that = (SuccessFile) o;
        return creationTime == that.creationTime && modificationTime == that.modificationTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(creationTime, modificationTime);
    }
}
