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

package org.apache.paimon.utils;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.CoreOptions.BLOB_DESCRIPTOR_PREFIX;

/** Utils for {@link BlobDescriptor}. */
public class BlobDescriptorUtils {

    public static void validateTableRoot(Path tableRoot, BlobDescriptor descriptor)
            throws IOException {
        URI root = tableRoot.toUri().normalize();
        URI blob;
        try {
            blob = new Path(descriptor.uri()).toUri().normalize();
        } catch (RuntimeException e) {
            throw new IOException("Invalid blob descriptor URI.", e);
        }

        if (!Objects.equals(normalizeScheme(root.getScheme()), normalizeScheme(blob.getScheme()))
                || !Objects.equals(root.getAuthority(), blob.getAuthority())) {
            throw new IOException(
                    "Blob descriptor URI must use the same scheme and authority as table root.");
        }

        String rootPath = root.getPath();
        String blobPath = blob.getPath();
        String childPrefix = rootPath.endsWith("/") ? rootPath : rootPath + "/";
        if (!blobPath.startsWith(childPrefix)) {
            throw new IOException("Blob descriptor URI must be under table root.");
        }
    }

    private static String normalizeScheme(@Nullable String scheme) {
        return scheme == null ? null : scheme.toLowerCase();
    }

    /**
     * Try to create a {@link CatalogContext} for input {@link BlobDescriptor}. This enables reading
     * descriptors from external storages which can be different from paimon's own.
     */
    public static CatalogContext getCatalogContext(
            @Nullable CatalogContext currentContext, Options tableOptions) {
        Map<String, String> descriptorSpecified = new HashMap<>();
        for (Map.Entry<String, String> entry : tableOptions.toMap().entrySet()) {
            String key = entry.getKey();
            if (key != null && key.startsWith(BLOB_DESCRIPTOR_PREFIX)) {
                descriptorSpecified.put(
                        key.substring(BLOB_DESCRIPTOR_PREFIX.length()), entry.getValue());
            }
        }

        if (descriptorSpecified.isEmpty()) {
            return currentContext == null ? CatalogContext.create(tableOptions) : currentContext;
        } else {
            return CatalogContext.create(Options.fromMap(descriptorSpecified));
        }
    }
}
