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

package org.apache.paimon.format;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/** Format Key for read a file. */
public class FormatKey {

    public final long schemaId;
    public final String format;
    @Nullable public final List<String> fieldNames;

    public FormatKey(long schemaId, String format) {
        this(schemaId, format, null);
    }

    public FormatKey(long schemaId, String format, @Nullable List<String> fieldNames) {
        this.schemaId = schemaId;
        this.format = format;
        this.fieldNames = fieldNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FormatKey formatKey = (FormatKey) o;
        return schemaId == formatKey.schemaId
                && Objects.equals(format, formatKey.format)
                && Objects.equals(fieldNames, formatKey.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaId, format, fieldNames);
    }
}
