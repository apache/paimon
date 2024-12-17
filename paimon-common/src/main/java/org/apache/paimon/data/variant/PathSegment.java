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

package org.apache.paimon.data.variant;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A path segment for variant get to represent either an object key access or an array index access.
 */
public class PathSegment {
    private final String key;
    private final Integer index;

    private PathSegment(String key, Integer index) {
        this.key = key;
        this.index = index;
    }

    public static PathSegment createKeySegment(String key) {
        return new PathSegment(key, null);
    }

    public static PathSegment createIndexSegment(int index) {
        return new PathSegment(null, index);
    }

    public boolean isKey() {
        return key != null;
    }

    public boolean isIndex() {
        return index != null;
    }

    public String getKey() {
        return key;
    }

    public Integer getIndex() {
        return index;
    }

    private static final Pattern ROOT_PATTERN = Pattern.compile("\\$");
    // Parse index segment like `[123]`.
    private static final Pattern INDEX_PATTERN = Pattern.compile("\\[(\\d+)]");
    // Parse key segment like `.name` or `['name']` or `["name"]`.
    private static final Pattern KEY_PATTERN =
            Pattern.compile("\\.([^.\\[]+)|\\['([^']+)']|\\[\"([^\"]+)\"]");

    public static PathSegment[] parse(String str) {
        // Validate root
        Matcher rootMatcher = ROOT_PATTERN.matcher(str);
        if (str.isEmpty() || !rootMatcher.find()) {
            throw new IllegalArgumentException("Invalid path: " + str);
        }

        List<PathSegment> segments = new ArrayList<>();
        String remaining = str.substring(rootMatcher.end());
        // Parse indexes and keys
        while (!remaining.isEmpty()) {
            Matcher indexMatcher = INDEX_PATTERN.matcher(remaining);
            if (indexMatcher.lookingAt()) {
                int index = Integer.parseInt(indexMatcher.group(1));
                segments.add(PathSegment.createIndexSegment(index));
                remaining = remaining.substring(indexMatcher.end());
                continue;
            }

            Matcher keyMatcher = KEY_PATTERN.matcher(remaining);
            if (keyMatcher.lookingAt()) {
                for (int i = 1; i <= 3; i++) {
                    if (keyMatcher.group(i) != null) {
                        segments.add(PathSegment.createKeySegment(keyMatcher.group(i)));
                        break;
                    }
                }
                remaining = remaining.substring(keyMatcher.end());
                continue;
            }
            throw new IllegalArgumentException("Invalid path: " + str);
        }

        return segments.toArray(new PathSegment[0]);
    }
}
