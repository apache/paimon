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

package org.apache.paimon.flink.sink;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** TableFilter is used to filter tables according to whitelist and prefix/suffix patterns. */
public class TableFilter implements java.io.Serializable {
    private String dbName;
    private List<String> tableWhitelist;
    private List<String> tablePrefixes;
    private List<String> tableSuffixes;
    private String tblIncludingPattern = ".*";
    private String tblExcludingPattern = "";

    public TableFilter(
            String dbName,
            List<String> tableWhitelist,
            List<String> tablePrefixes,
            List<String> tableSuffixes,
            String tblIncludingPattern,
            String tblExcludingPattern) {
        this.dbName = dbName;
        this.tableWhitelist = tableWhitelist;
        this.tablePrefixes = tablePrefixes;
        this.tableSuffixes = tableSuffixes;
        this.tblIncludingPattern = tblIncludingPattern;
        this.tblExcludingPattern = tblExcludingPattern;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public List<String> getTableWhitelist() {
        return tableWhitelist;
    }

    public void setTableWhitelist(List<String> tableWhitelist) {
        this.tableWhitelist = tableWhitelist;
    }

    public List<String> getTablePrefixes() {
        return tablePrefixes;
    }

    public void setTablePrefixes(List<String> tablePrefixes) {
        this.tablePrefixes = tablePrefixes;
    }

    public List<String> getTableSuffixes() {
        return tableSuffixes;
    }

    public void setTableSuffixes(List<String> tableSuffixes) {
        this.tableSuffixes = tableSuffixes;
    }

    public String getTblIncludingPattern() {
        return tblIncludingPattern;
    }

    public void setTblIncludingPattern(String tblIncludingPattern) {
        this.tblIncludingPattern = tblIncludingPattern;
    }

    public String getTblExcludingPattern() {
        return tblExcludingPattern;
    }

    public void setTblExcludingPattern(String tblExcludingPattern) {
        this.tblExcludingPattern = tblExcludingPattern;
    }

    public List<String> filterTables(List<String> allTables) {
        List<String> inPatternList = Arrays.asList(tblIncludingPattern.split("\\|"));
        List<String> exPatternList =
                (tblExcludingPattern == null || tblExcludingPattern.isEmpty())
                        ? Collections.emptyList()
                        : Arrays.asList(tblExcludingPattern.split("\\|"));
        String inPattern =
                inPatternList.stream()
                        .flatMap(
                                p ->
                                        tablePrefixes.isEmpty()
                                                ? Stream.of(p)
                                                : tablePrefixes.stream().map(prefix -> prefix + p))
                        .flatMap(
                                p ->
                                        tableSuffixes.isEmpty()
                                                ? Stream.of(p)
                                                : tableSuffixes.stream().map(suffix -> p + suffix))
                        .collect(Collectors.joining("|"));

        String exPattern =
                exPatternList.isEmpty()
                        ? ""
                        : exPatternList.stream()
                                .flatMap(
                                        p ->
                                                tablePrefixes.isEmpty()
                                                        ? Stream.of(p)
                                                        : tablePrefixes.stream()
                                                                .map(prefix -> prefix + p))
                                .flatMap(
                                        p ->
                                                tableSuffixes.isEmpty()
                                                        ? Stream.of(p)
                                                        : tableSuffixes.stream()
                                                                .map(suffix -> p + suffix))
                                .collect(Collectors.joining("|"));

        return allTables.stream()
                .filter(t -> exPattern.isEmpty() || !t.matches(exPattern))
                .filter(t -> tableWhitelist.contains(t) || t.matches(inPattern))
                .collect(Collectors.toList());
    }
}
