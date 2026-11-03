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

package org.apache.paimon.fs;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static org.apache.paimon.fs.EntropyInjectExternalPathProvider.computeHash;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Provider for external data paths which injects the hash directories into a path template.
 *
 * <p>{@link EntropyInjectExternalPathProvider} appends the hash directories after the table
 * context, so the object keys of a table all share one table-specific prefix and the storage
 * partitions warmed up for that prefix cannot be shared with other tables. This provider instead
 * replaces the {@value #HASH_PLACEHOLDER} placeholder inside the configured template, which lets
 * the hash appear before the database and table directories:
 *
 * <pre>
 * template: s3://bucket-a/{hash}/my_db.db/my_table
 * expanded: s3://bucket-a/1001/1001/1001/10011001/my_db.db/my_table
 * file:     s3://bucket-a/1001/1001/1001/10011001/my_db.db/my_table/dt=2026-11-02/bucket-0/data-xxx.parquet
 * </pre>
 *
 * <p>Multiple databases and tables pointing at the same template root therefore share the same hash
 * prefixes.
 *
 * <p>Only a single template is accepted. The placeholder is replaced in place with hash directories
 * computed from the file name. No path rotation or mutable state is needed.
 */
@ThreadSafe
public class TemplateEntropyInjectExternalPathProvider implements ExternalPathProvider {

    public static final String HASH_PLACEHOLDER = "{hash}";

    private final Path externalTablePathTemplate;
    private final Path relativeBucketPath;

    public TemplateEntropyInjectExternalPathProvider(
            List<Path> externalTablePathTemplates, Path relativeBucketPath) {
        checkArgument(
                externalTablePathTemplates.size() == 1,
                "Only a single external path template is supported, but got %s.",
                externalTablePathTemplates);
        Path template = externalTablePathTemplates.get(0);
        checkTemplate(template);
        this.externalTablePathTemplate = template;
        this.relativeBucketPath = relativeBucketPath;
    }

    @Override
    public Path getNextExternalDataPath(String fileName) {
        Path externalTablePath = expand(externalTablePathTemplate, fileName);
        return new Path(new Path(externalTablePath, relativeBucketPath), fileName);
    }

    /** Whether the given external path is a template handled by this provider. */
    public static boolean isTemplate(String externalPath) {
        return externalPath.contains(HASH_PLACEHOLDER);
    }

    private static void checkTemplate(Path template) {
        checkArgument(
                isTemplate(template.toString()),
                "External path '%s' must contain the '%s' placeholder.",
                template,
                HASH_PLACEHOLDER);
    }

    private static Path expand(Path template, String fileName) {
        return new Path(template.toString().replace(HASH_PLACEHOLDER, computeHash(fileName)));
    }
}
