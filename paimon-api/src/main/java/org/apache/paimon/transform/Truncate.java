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

package org.apache.paimon.transform;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;

import javax.annotation.Nullable;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Using truncate strategy to transform to bucket. */
public abstract class Truncate<S> implements BucketStrategy<S> {

    public static final Pattern TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");

    protected final int width;

    public Truncate(int width) {
        checkArgument(width > 0, "Invalid truncate width: %s (must be > 0)", width);
        this.width = width;
    }

    public int getWidth() {
        return width;
    }

    @Override
    public String toString() {
        return "truncate[width=" + width + "]";
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public static <S> Truncate<S> fromString(
            String bucketStrategy, List<DataField> bucketColumns, int numBuckets) {
        checkArgument(numBuckets > 0, "numBuckets must be greater than 0");
        Matcher widthMatcher = TRUNCATE_PATTERN.matcher(bucketStrategy);
        if (widthMatcher.matches()) {
            checkArgument(
                    bucketColumns.size() == 1,
                    "bucket key columns must contain exactly one column for truncate bucket strategy.");
            int width = Integer.parseInt(widthMatcher.group(1));
            DataType dataType = bucketColumns.get(0).type();
            if (dataType.getTypeRoot() == DataTypeRoot.INTEGER) {
                return (Truncate<S>) new TruncateInteger(width);
            } else {
                throw new IllegalArgumentException("Cannot truncate type: " + dataType);
            }
        }
        return null;
    }

    private static class TruncateInteger extends Truncate<Integer> {
        private static final long serialVersionUID = 1L;

        private TruncateInteger(int width) {
            super(width);
        }

        @Override
        public int apply(Integer value) {
            checkNotNull(value, "The source value to do bucket transform must not be null");

            return value - (((value % width) + width) % width);
        }
    }
}
