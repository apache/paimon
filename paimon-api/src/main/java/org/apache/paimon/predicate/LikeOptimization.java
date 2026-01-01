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

import org.apache.paimon.utils.Pair;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Try to optimize like to startsWith, endsWith, contains or equals. */
public class LikeOptimization {

    /** Accepts simple LIKE patterns like "abc%". */
    private static final Pattern BEGIN_PATTERN = Pattern.compile("([^%]+)%");
    /** Accepts simple LIKE patterns like "%abc". */
    private static final Pattern END_PATTERN = Pattern.compile("%([^%]+)");
    /** Accepts simple LIKE patterns like "%abc%". */
    private static final Pattern MIDDLE_PATTERN = Pattern.compile("%([^%]+)%");
    /** Accepts simple LIKE patterns like "abc". */
    private static final Pattern NONE_PATTERN = Pattern.compile("[^%]+");

    public static Optional<Pair<NullFalseLeafBinaryFunction, Object>> tryOptimize(
            Object patternLiteral) {
        if (patternLiteral == null) {
            throw new IllegalArgumentException("Pattern can not be null.");
        }

        String pattern = patternLiteral.toString();
        if (pattern.contains("_")) {
            return Optional.empty();
        }

        Matcher noneMatcher = NONE_PATTERN.matcher(pattern);
        Matcher beginMatcher = BEGIN_PATTERN.matcher(pattern);
        Matcher endMatcher = END_PATTERN.matcher(pattern);
        Matcher middleMatcher = MIDDLE_PATTERN.matcher(pattern);

        if (noneMatcher.matches()) {
            return Optional.of(Pair.of(Equal.INSTANCE, pattern));
        } else if (beginMatcher.matches()) {
            return Optional.of(Pair.of(StartsWith.INSTANCE, beginMatcher.group(1)));
        } else if (endMatcher.matches()) {
            return Optional.of(Pair.of(EndsWith.INSTANCE, endMatcher.group(1)));
        } else if (middleMatcher.matches()) {
            return Optional.of(Pair.of(Contains.INSTANCE, middleMatcher.group(1)));
        } else {
            return Optional.empty();
        }
    }
}
