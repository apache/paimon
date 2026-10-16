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

package org.apache.paimon.rest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** The operations of the REST OpenAPI specs in docs/static, each as "METHOD /path/{template}". */
public final class OpenApiSpec {

    private static final List<String> FILES =
            Arrays.asList("rest-catalog-open-api.yaml", "rest-management-open-api.yaml");
    private static final List<String> METHODS =
            Arrays.asList("get", "put", "post", "delete", "patch", "head");

    private OpenApiSpec() {}

    public static Set<String> operations() throws IOException {
        File directory = specDirectory();
        Set<String> operations = new TreeSet<>();
        for (String file : FILES) {
            Iterator<Map.Entry<String, JsonNode>> paths =
                    new YAMLMapper().readTree(new File(directory, file)).get("paths").fields();
            while (paths.hasNext()) {
                Map.Entry<String, JsonNode> path = paths.next();
                Iterator<String> methods = path.getValue().fieldNames();
                while (methods.hasNext()) {
                    String method = methods.next();
                    if (METHODS.contains(method)) {
                        operations.add(method.toUpperCase(Locale.ROOT) + " " + path.getKey());
                    }
                }
            }
        }
        return operations;
    }

    /** Returns the operation a request matches, preferring literal segments, or null. */
    @Nullable
    public static String match(Set<String> operations, String method, String path) {
        String[] segments = path.split("/", -1);
        String best = null;
        int bestLiterals = -1;
        for (String operation : operations) {
            String[] parts = operation.split(" ", 2);
            String[] template = parts[1].split("/", -1);
            if (!parts[0].equals(method) || template.length != segments.length) {
                continue;
            }
            int literals = 0;
            boolean matches = true;
            for (int i = 0; i < template.length && matches; i++) {
                if (template[i].startsWith("{")) {
                    matches = !segments[i].isEmpty();
                } else {
                    matches = template[i].equals(segments[i]);
                    literals++;
                }
            }
            if (matches && literals > bestLiterals) {
                best = operation;
                bestLiterals = literals;
            }
        }
        return best;
    }

    /** Replaces each {name} with {} so templates compare whatever their parameters are called. */
    public static String normalize(String operation) {
        return operation.replaceAll("\\{[^}]*}", "{}");
    }

    private static File specDirectory() {
        for (File dir = new File("").getAbsoluteFile(); dir != null; dir = dir.getParentFile()) {
            File candidate = new File(dir, "docs/static");
            if (new File(candidate, FILES.get(0)).isFile()) {
                return candidate;
            }
        }
        throw new IllegalStateException("docs/static/" + FILES.get(0) + " not found");
    }
}
