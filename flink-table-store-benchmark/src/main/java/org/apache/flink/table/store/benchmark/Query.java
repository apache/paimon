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

package org.apache.flink.table.store.benchmark;

import org.apache.flink.table.store.benchmark.utils.BenchmarkUtils;
import org.apache.flink.util.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Benchmark query. */
public class Query {

    private static final Pattern SINK_DDL_TEMPLATE_PATTERN =
            Pattern.compile("-- __SINK_DDL_BEGIN__([\\s\\S]*?)-- __SINK_DDL_END__");

    private final String name;
    private final String beforeSql;
    private final String querySql;
    private final boolean bounded;

    private Query(String name, String beforeSql, String querySql, boolean bounded) {
        this.name = name;
        this.beforeSql = beforeSql;
        this.querySql = querySql;
        this.bounded = bounded;
    }

    public String name() {
        return name;
    }

    public boolean bounded() {
        return bounded;
    }

    public Optional<String> getBeforeSql(Sink sink, String sinkPath) {
        return Optional.ofNullable(beforeSql)
                .map(sql -> getSql(sink.beforeSql() + "\n" + sql, sink, sinkPath));
    }

    public String getWriteBenchmarkSql(Sink sink, String sinkPath) {
        return getSql(sink.beforeSql() + "\n" + querySql, sink, sinkPath);
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
        Matcher m = SINK_DDL_TEMPLATE_PATTERN.matcher(querySql);
        if (!m.find()) {
            throw new IllegalArgumentException(
                    "Cannot find __SINK_DDL_BEGIN__ and __SINK_DDL_END__ in query "
                            + name
                            + ". This query is not valid.");
        }
        return m.group(1)
                .replace("${SINK_NAME}", tableName)
                .replace("${DDL_TEMPLATE}", tableProperties);
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
            String beforeSqlFileName = (String) queryMap.get("before");
            boolean bounded =
                    queryMap.containsKey("bounded")
                            && Boolean.parseBoolean(queryMap.get("bounded").toString());
            result.add(
                    new Query(
                            name,
                            beforeSqlFileName == null
                                    ? null
                                    : FileUtils.readFileUtf8(
                                            queryLocation.resolve(beforeSqlFileName).toFile()),
                            FileUtils.readFileUtf8(queryLocation.resolve(name + ".sql").toFile()),
                            bounded));
        }
        return result;
    }
}
