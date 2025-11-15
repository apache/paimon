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

package org.apache.paimon.index.globalindex;

import org.apache.paimon.utils.Range;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public interface GlobalIndexResult {

    GlobalIndexResult ALL =
            new GlobalIndexResult() {
                @Override
                public GlobalIndexResult and(GlobalIndexResult fileIndexResult) {
                    return fileIndexResult;
                }

                @Override
                public GlobalIndexResult or(GlobalIndexResult fileIndexResult) {
                    return this;
                }

                @Override
                public boolean empty() {
                    return false;
                }

                @Override
                public Iterator<Range> results() {
                    throw new UnsupportedOperationException(
                            "ALL result does not support results()");
                }
            };

    GlobalIndexResult NONE =
            new GlobalIndexResult() {
                @Override
                public GlobalIndexResult and(GlobalIndexResult fileIndexResult) {
                    return this;
                }

                @Override
                public GlobalIndexResult or(GlobalIndexResult fileIndexResult) {
                    return fileIndexResult;
                }

                @Override
                public boolean empty() {
                    return true;
                }

                @Override
                public Iterator<Range> results() {
                    return Collections.emptyIterator();
                }
            };

    default GlobalIndexResult and(GlobalIndexResult fileIndexResult) {
        if (empty() || fileIndexResult.empty()) {
            return NONE;
        }

        Iterator<Range> it1 = results();
        Iterator<Range> it2 = fileIndexResult.results();

        Set<Range> intersection = RangeUtils.and(it1, it2);

        return wrap(intersection);
    }

    default GlobalIndexResult or(GlobalIndexResult fileIndexResult) {
        if (empty()) {
            return fileIndexResult;
        }
        if (fileIndexResult.empty()) {
            return this;
        }

        Iterator<Range> it1 = results();
        Iterator<Range> it2 = fileIndexResult.results();

        Set<Range> intersection = RangeUtils.or(it1, it2);

        return wrap(intersection);
    }

    boolean empty();

    Iterator<Range> results();

    static GlobalIndexResult wrap(final Set<Range> results) {
        return new GlobalIndexResult() {
            @Override
            public boolean empty() {
                return results.isEmpty();
            }

            @Override
            public Iterator<Range> results() {
                return results.iterator();
            }
        };
    }
}
