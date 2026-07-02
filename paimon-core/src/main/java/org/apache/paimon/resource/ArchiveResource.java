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

package org.apache.paimon.resource;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;

import javax.annotation.Nullable;

/** A {@link Resource} implementation for archive resources (e.g., zip, tar). */
public class ArchiveResource extends AbstractResource {

    private static final long serialVersionUID = 1L;

    public ArchiveResource(
            Identifier identifier,
            @Nullable String comment,
            String uri,
            long size,
            long lastModifiedTime,
            FileIO fileIO) {
        super(identifier, comment, uri, size, lastModifiedTime, fileIO);
    }

    @Override
    public ResourceType resourceType() {
        return ResourceType.ARCHIVE;
    }
}
