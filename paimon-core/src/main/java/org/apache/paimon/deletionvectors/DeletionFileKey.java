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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.utils.Range;

import java.io.Serializable;
import java.util.Collection;

/** Key for associating a data deletion vector with data. */
public abstract class DeletionFileKey implements Serializable {

    private static final long serialVersionUID = 1L;

    public static FileNameKey ofFileName(String fileName) {
        return new FileNameKey(fileName);
    }

    public static RowIdRangeKey ofRange(Range range) {
        return new RowIdRangeKey(range);
    }

    public static Type checkType(Collection<DeletionFileKey> keys) {
        if (keys == null || keys.isEmpty()) {
            throw new RuntimeException("Empty keys.");
        }

        Type type = null;
        for (DeletionFileKey key : keys) {
            if (type == null) {
                type = key.type();
            } else if (type != key.type()) {
                throw new IllegalStateException(
                        "All DeletionFileKeys should always be the same type, it's a bug.");
            }
        }

        return type;
    }

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();

    @Override
    public abstract String toString();

    public abstract Type type();

    /** Type of this key. */
    public enum Type {
        FILE_NAME,
        ROW_RANGE
    }
}
