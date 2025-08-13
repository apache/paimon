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

package org.apache.paimon.partition;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Utils class for partition value generation. */
public class PartitionValueGenerator {

    private final String pattern;
    private final String formatter;

    public PartitionValueGenerator(@Nullable String pattern, @Nullable String formatter) {
        this.pattern = pattern;
        this.formatter = formatter;
    }

    public List<String> generatePartitionValues(
            LocalDateTime dateTime, List<String> partitionKeys) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(this.formatter);
        String formattedDateTime = dateTime.format(formatter);
        Pattern keyPattern = Pattern.compile("\\$(\\w+)");
        Matcher keyMatcher = keyPattern.matcher(this.pattern);
        List<String> keyOrder = new ArrayList<>();
        StringBuilder regexBuilder = new StringBuilder();
        int lastPosition = 0;
        while (keyMatcher.find()) {
            regexBuilder.append(
                    Pattern.quote(this.pattern.substring(lastPosition, keyMatcher.start())));
            regexBuilder.append("(.+)");
            keyOrder.add(keyMatcher.group(1));
            lastPosition = keyMatcher.end();
        }
        regexBuilder.append(Pattern.quote(this.pattern.substring(lastPosition)));

        Matcher valueMatcher = Pattern.compile(regexBuilder.toString()).matcher(formattedDateTime);
        if (!valueMatcher.matches() || valueMatcher.groupCount() != keyOrder.size()) {
            throw new IllegalArgumentException(
                    "Formatted datetime does not match timestamp pattern");
        }

        Map<String, String> keyValues = new HashMap<>();
        for (int i = 0; i < keyOrder.size(); i++) {
            keyValues.put(keyOrder.get(i), valueMatcher.group(i + 1));
        }
        return partitionKeys.stream()
                .map(key -> keyValues.getOrDefault(key, ""))
                .collect(Collectors.toList());
    }
}
