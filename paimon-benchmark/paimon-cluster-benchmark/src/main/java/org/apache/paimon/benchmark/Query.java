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

package org.apache.paimon.benchmark;

import org.apache.paimon.benchmark.utils.BenchmarkUtils;

import org.apache.flink.util.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Benchmark query. */
public class Query {

    private static final Pattern SINK_DDL_TEMPLATE_PATTERN =
            Pattern.compile("-- __SINK_DDL_BEGIN__([\\s\\S]*?)-- __SINK_DDL_END__");

    private final String name;
    private final List<String> writeSqlName;
    private final List<String> writeSqlText;
    private final List<Long> rowNum;

    private Query(
            String name, List<String> writeSqlName, List<String> writeSqlText, List<Long> rowNum) {
        this.name = name;
        this.writeSqlName = writeSqlName;
        this.writeSqlText = writeSqlText;
        this.rowNum = rowNum;
    }

    public String name() {
        return name;
    }

    public List<WriteSql> getWriteBenchmarkSql(Sink sink, String sinkPath) {
        List<WriteSql> res = new ArrayList<>();
        for (int i = 0; i < writeSqlName.size(); i++) {
            res.add(
                    new WriteSql(
                            writeSqlName.get(i),
                            getSql(sink.beforeSql() + "\n" + writeSqlText.get(i), sink, sinkPath),
                            rowNum.get(i)));
        }
        return res;
    }

    public String getReadBenchmarkSql(Sink sink, String sinkPath) {
        return getSql(
                String.join(
                        "\n",
                        "SET 'execution.runtime-mode' = 'batch';",
                        sink.beforeSql(),
                        getSinkDdl(sink.tableName(), sink.tableProperties()),
                        getSinkDdl("B", "'connector' = 'blackhole'"),
                        "INSERT INTO B SELECT * FROM " + sink.tableName() + ";"),
                sink,
                sinkPath);
    }

    private String getSinkDdl(String tableName, String tableProperties) {
        for (String s : writeSqlText) {
            Matcher m = SINK_DDL_TEMPLATE_PATTERN.matcher(s);
            if (m.find()) {
                return m.group(1)
                        .replace("${SINK_NAME}", tableName)
                        .replace("${DDL_TEMPLATE}", tableProperties);
            }
        }
        throw new IllegalArgumentException(
                "Cannot find __SINK_DDL_BEGIN__ and __SINK_DDL_END__ in query "
                        + name
                        + ". This query is not valid.");
    }

    private String getSql(String sqlTemplate, Sink sink, String sinkPath) {
        return sqlTemplate
                .replace("${SINK_NAME}", sink.tableName())
                .replace("${DDL_TEMPLATE}", sink.tableProperties())
                .replace("${SINK_PATH}", sinkPath);
    }

    public static List<Query> load(Path location) throws IOException {
        Path queryLocation = location.resolve("queries");
        Map<?, ?> yaml =
                BenchmarkUtils.YAML_MAPPER.readValue(
                        FileUtils.readFileUtf8(queryLocation.resolve("queries.yaml").toFile()),
                        Map.class);

        List<Query> result = new ArrayList<>();
        for (Map.Entry<?, ?> entry : yaml.entrySet()) {
            String name = (String) entry.getKey();
            Map<?, ?> queryMap = (Map<?, ?>) entry.getValue();
            List<String> writeSqlName = new ArrayList<>();
            List<String> writeSqlText = new ArrayList<>();
            for (Object filename : (List<?>) queryMap.get("sql")) {
                writeSqlName.add(filename.toString());
                writeSqlText.add(
                        FileUtils.readFileUtf8(
                                queryLocation.resolve(filename.toString()).toFile()));
            }
            List<Long> rowNum = new ArrayList<>();
            for (Object rowNumStr : (List<?>) queryMap.get("row-num")) {
                rowNum.add(Long.valueOf(rowNumStr.toString()));
            }
            result.add(new Query(name, writeSqlName, writeSqlText, rowNum));
        }
        return result;
    }

    /** Sql for write benchmarks. */
    public static class WriteSql {
        public final String name;
        public final String text;
        public final long rowNum;

        private WriteSql(String name, String text, long rowNum) {
            this.name = name;
            this.text = text;
            this.rowNum = rowNum;
        }
    }
}
