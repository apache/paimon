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

package org.apache.paimon.globalindex;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.globalindex.EsSearchQuery.integer;
import static org.apache.paimon.globalindex.EsSearchQuery.only;
import static org.apache.paimon.globalindex.EsSearchQuery.require;
import static org.apache.paimon.globalindex.EsSearchQuery.text;

/** Engine-independent, single-target SEARCH request for an ESM1 global index. */
public final class EsSearchRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String target;
    public final String database;
    public final String table;
    @Nullable public final Long snapshotId;
    public final List<Integer> indexFields;
    public final Map<String, Integer> bindings;
    public final EsSearchQuery query;
    public final int from;
    public final int size;
    public final boolean fetchSource;

    private EsSearchRequest(JsonNode root) {
        only(root, "version", "targets", "operation", "request", "options");
        integer(root.get("version"), 1, 1);
        require(
                text(root.get("operation")).equalsIgnoreCase("SEARCH"),
                "ESM1 paimon_es_search currently supports SEARCH only");
        if (root.has("options")) {
            JsonNode options = root.get("options");
            only(options, "scoring");
            require(
                    !options.has("scoring")
                            || text(options.get("scoring")).equalsIgnoreCase("QUERY_THEN_FETCH"),
                    "ESM1 supports shard-local QUERY_THEN_FETCH scoring only");
        }
        JsonNode targets = root.get("targets");
        require(
                targets != null && targets.isArray() && targets.size() == 1,
                "ESM1 paimon_es_search requires exactly one target");
        JsonNode target = targets.get(0);
        only(target, "name", "database", "table", "branch", "snapshot", "indexes", "bindings");
        require(
                !target.has("branch") || text(target.get("branch")).equals("main"),
                "This ESM1 entry currently supports the main branch only");
        this.target = text(target.get("name"));
        this.database = text(target.get("database"));
        this.table = text(target.get("table"));
        JsonNode indexes = target.get("indexes");
        require(
                indexes != null
                        && indexes.isArray()
                        && indexes.size() == 1
                        && indexes.get(0).isArray()
                        && indexes.get(0).size() > 0
                        && indexes.get(0).size() <= 1024,
                "Select exactly one nonempty grouped ESM1 index");
        List<Integer> ids = new ArrayList<>();
        for (JsonNode id : indexes.get(0)) {
            ids.add(integer(id, 0, Integer.MAX_VALUE));
        }
        require(new HashSet<>(ids).size() == ids.size(), "Duplicate index field IDs");
        this.indexFields = Collections.unmodifiableList(ids);
        JsonNode aliases = target.get("bindings");
        require(
                aliases != null && aliases.isObject() && aliases.size() <= 1024,
                "Expected bounded field bindings");
        Map<String, Integer> bindings = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> entries = aliases.fields();
        while (entries.hasNext()) {
            Map.Entry<String, JsonNode> entry = entries.next();
            int id = integer(entry.getValue(), 0, Integer.MAX_VALUE);
            require(ids.contains(id), "Binding refers to a field outside the selected index");
            bindings.put(entry.getKey(), id);
        }
        this.bindings = Collections.unmodifiableMap(bindings);
        this.snapshotId = snapshot(target);
        JsonNode request = root.get("request");
        only(request, "query", "from", "size", "fetch");
        this.from = request.has("from") ? integer(request.get("from"), 0, 9999) : 0;
        this.size = request.has("size") ? integer(request.get("size"), 1, 10000 - from) : 10;
        require(from + size <= 10000, "Search window exceeds 10000");
        require(request.has("query"), "Missing query");
        this.query = new EsSearchQuery(request.get("query").toString(), from + size);
        require(bindings.keySet().containsAll(query.fields()), "Every query field needs a binding");
        String fetch = request.has("fetch") ? text(request.get("fetch")) : "NONE";
        require(
                "NONE".equalsIgnoreCase(fetch) || "SOURCE".equalsIgnoreCase(fetch),
                "fetch must be NONE or SOURCE");
        this.fetchSource = "SOURCE".equalsIgnoreCase(fetch);
    }

    @Nullable
    private static Long snapshot(JsonNode target) {
        if (!target.has("snapshot")) {
            return null;
        }
        JsonNode selector = target.get("snapshot");
        only(selector, "kind", "snapshot_id", "tag");
        require(
                selector.has("snapshot_id") && selector.has("tag") && selector.get("tag").isNull(),
                "Use an explicit snapshot ID or latest; tags are not supported by this entry");
        switch (text(selector.get("kind"))) {
            case "latest":
                require(
                        selector.get("snapshot_id").isNull(),
                        "latest cannot include a snapshot ID");
                return null;
            case "snapshot":
                String value = text(selector.get("snapshot_id"));
                require(value.matches("[1-9][0-9]*"), "Snapshot IDs are positive decimal strings");
                long id = Long.parseLong(value);
                require(id > 0, "Snapshot ID must be positive");
                return id;
            default:
                throw new IllegalArgumentException("Unsupported snapshot selector");
        }
    }

    public static EsSearchRequest parse(String json) {
        return new EsSearchRequest(EsSearchQuery.parse(json));
    }
}
