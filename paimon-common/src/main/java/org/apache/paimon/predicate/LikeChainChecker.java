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

package org.apache.paimon.predicate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.memory.MemorySegmentUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Like matcher for patterns containing only literal text and % wildcards and no escape characters.
 * Avoids the regex engine for %% clauses.
 *
 * <p>Algorithm to tokenise the pattern into literal segments split by %, anchor the first/last
 * segment if the pattern lacks a leading/trailing %, and walk the input via byte level for each
 * interior segment.
 */
public final class LikeChainChecker {

    private final BinaryString[] segments;
    private final boolean leftAnchored;
    private final boolean rightAnchored;
    private final int minLen;

    private LikeChainChecker(BinaryString[] segments, boolean leftAnchored, boolean rightAnchored) {
        this.segments = segments;
        this.leftAnchored = leftAnchored;
        this.rightAnchored = rightAnchored;
        int sum = 0;
        for (BinaryString seg : segments) {
            sum += seg.getSizeInBytes();
        }
        this.minLen = sum;
    }

    public static LikeChainChecker compile(String pattern) {
        if (pattern.indexOf('_') >= 0 || pattern.indexOf('\\') >= 0) {
            return null;
        }
        boolean leftAnchored = pattern.isEmpty() || pattern.charAt(0) != '%';
        boolean rightAnchored = pattern.isEmpty() || pattern.charAt(pattern.length() - 1) != '%';

        List<BinaryString> parts = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < pattern.length(); i++) {
            if (pattern.charAt(i) == '%') {
                if (i > start) {
                    parts.add(BinaryString.fromString(pattern.substring(start, i)));
                }
                start = i + 1;
            }
        }
        if (start < pattern.length()) {
            parts.add(BinaryString.fromString(pattern.substring(start)));
        }
        return new LikeChainChecker(
                parts.toArray(new BinaryString[0]), leftAnchored, rightAnchored);
    }

    public boolean test(BinaryString input) {
        int inputLen = input.getSizeInBytes();
        if (inputLen < minLen) {
            return false;
        }
        if (segments.length == 0) {
            return !(leftAnchored && rightAnchored) || inputLen == 0;
        }

        if (segments.length == 1) {
            BinaryString only = segments[0];
            if (leftAnchored && rightAnchored) {
                return inputLen == only.getSizeInBytes() && input.startsWith(only);
            } else if (leftAnchored) {
                return input.startsWith(only);
            } else if (rightAnchored) {
                return input.endsWith(only);
            } else {
                return findIn(input, only, 0) >= 0;
            }
        }

        int pos = 0;

        BinaryString first = segments[0];
        if (leftAnchored) {
            if (!input.startsWith(first)) {
                return false;
            }
            pos = first.getSizeInBytes();
        } else {
            int found = findIn(input, first, 0);
            if (found < 0) {
                return false;
            }
            pos = found + first.getSizeInBytes();
        }

        int lastIdx = segments.length - 1;
        for (int i = 1; i < lastIdx; i++) {
            BinaryString seg = segments[i];
            int found = findIn(input, seg, pos);
            if (found < 0) {
                return false;
            }
            pos = found + seg.getSizeInBytes();
        }

        BinaryString last = segments[lastIdx];
        int lastLen = last.getSizeInBytes();
        if (rightAnchored) {
            if (!input.endsWith(last)) {
                return false;
            }
            int lastStart = inputLen - lastLen;
            if (lastStart < pos) {
                return false;
            }
        } else {
            if (findIn(input, last, pos) < 0) {
                return false;
            }
        }

        return true;
    }

    private static int findIn(BinaryString input, BinaryString needle, int fromByte) {
        int inputLen = input.getSizeInBytes();
        int needleLen = needle.getSizeInBytes();
        if (needleLen == 0) {
            return fromByte;
        }
        if (fromByte + needleLen > inputLen) {
            return -1;
        }
        int absoluteOffset =
                MemorySegmentUtils.find(
                        input.getSegments(),
                        input.getOffset() + fromByte,
                        inputLen - fromByte,
                        needle.getSegments(),
                        needle.getOffset(),
                        needleLen);
        if (absoluteOffset < 0) {
            return -1;
        }
        return absoluteOffset - input.getOffset();
    }
}
