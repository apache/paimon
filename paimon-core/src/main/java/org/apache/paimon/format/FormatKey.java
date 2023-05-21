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

import java.util.Objects;

/** Format Key for read a file. */
public class FormatKey {

    public final long schemaId;
    public final String format;

    public FormatKey(long schemaId, String format) {
        this.schemaId = schemaId;
        this.format = format;
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
        return schemaId == formatKey.schemaId && Objects.equals(format, formatKey.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaId, format);
    }
}
