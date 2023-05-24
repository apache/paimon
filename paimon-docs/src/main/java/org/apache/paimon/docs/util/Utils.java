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

package org.apache.paimon.docs.util;

/** Contains various shared utility functions. */
public class Utils {

    /**
     * Placeholder that is used to prevent certain sections from being escaped. We don't need a
     * sophisticated value but only something that won't show up in config options.
     */
    private static final String TEMPORARY_PLACEHOLDER = "superRandomTemporaryPlaceholder";

    public static String escapeCharacters(String value) {
        return value.replaceAll("<wbr>", TEMPORARY_PLACEHOLDER)
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;")
                .replaceAll(TEMPORARY_PLACEHOLDER, "<wbr>");
    }

    public static String apacheHeaderToHtml() {
        return "{{/*\n"
                + "Licensed to the Apache Software Foundation (ASF) under one\n"
                + "or more contributor license agreements.  See the NOTICE file\n"
                + "distributed with this work for additional information\n"
                + "regarding copyright ownership.  The ASF licenses this file\n"
                + "to you under the Apache License, Version 2.0 (the\n"
                + "\"License\"); you may not use this file except in compliance\n"
                + "with the License.  You may obtain a copy of the License at\n"
                + "\n"
                + "  http://www.apache.org/licenses/LICENSE-2.0\n"
                + "\n"
                + "Unless required by applicable law or agreed to in writing,\n"
                + "software distributed under the License is distributed on an\n"
                + "\"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n"
                + "KIND, either express or implied.  See the License for the\n"
                + "specific language governing permissions and limitations\n"
                + "under the License.\n"
                + "*/}}\n";
    }
}
