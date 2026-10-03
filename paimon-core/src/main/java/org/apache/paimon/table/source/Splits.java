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

package org.apache.paimon.table.source;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.globalindex.IndexQuerySplit;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackSplit;

import javax.annotation.Nullable;

/** Utilities for looking at what a planned {@link Split} is built from. */
public class Splits {

    private Splits() {}

    /**
     * The split a planned one is built from, looked at through every wrapper. One layer is not
     * enough: an authorized fallback table hands out a {@link FallbackSplit} holding a {@link
     * QueryAuthSplit}.
     *
     * <p>Stops at a {@link DataSplit}. {@code FallbackDataSplit} is one and wraps itself, so that
     * condition ends the loop as much as it preserves what a plan carrying no rules resolved to.
     *
     * <p>Only for reading what the split is made of. A caller handing it to a read passes the
     * planned split on as it is, or the authorization never reaches the reader.
     */
    public static Split underlying(Split split) {
        Split current = split;
        while (!(current instanceof DataSplit)) {
            if (current instanceof QueryAuthSplit) {
                current = ((QueryAuthSplit) current).split();
            } else if (current instanceof FallbackSplit) {
                current = ((FallbackSplit) current).wrapped();
            } else if (current instanceof IndexQuerySplit) {
                current = ((IndexQuerySplit) current).dataSplit();
            } else if (current instanceof IndexedSplit) {
                current = ((IndexedSplit) current).dataSplit();
            } else {
                return current;
            }
        }
        return current;
    }

    /**
     * The authorization a planned split carries, or null when it carries none. The companion of
     * {@link #underlying(Split)}, which drops the wrapper the rules live on.
     *
     * <p>A {@code FallbackDataSplit} wraps itself, which ends the walk: it carries no rules either.
     */
    @Nullable
    public static TableQueryAuthResult authResult(Split split) {
        Split current = split;
        while (!(current instanceof QueryAuthSplit)) {
            if (!(current instanceof FallbackSplit)) {
                return null;
            }
            Split wrapped = ((FallbackSplit) current).wrapped();
            if (wrapped == current) {
                return null;
            }
            current = wrapped;
        }
        return ((QueryAuthSplit) current).authResult();
    }
}
