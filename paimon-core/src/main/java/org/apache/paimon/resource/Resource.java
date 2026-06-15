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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.UriReaderFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * A resource provides basic abstraction for external resources managed by Paimon, such as files,
 * archives, JARs, and Python scripts.
 *
 * @since 0.4.0
 */
@Public
public interface Resource extends Serializable {

    /** A name to identify this resource. */
    String name();

    /** Full name of the resource, default is database.resourceName. */
    String fullName();

    /** Optional comment describing this resource. */
    Optional<String> comment();

    /** The URI pointing to the location of this resource. */
    String uri();

    /** The size of this resource in bytes. */
    long size();

    /** The last modified time of this resource in milliseconds since epoch. */
    long lastModifiedTime();

    /** The type of this resource. */
    ResourceType resourceType();

    /** Returns the contents of this resource as bytes. */
    byte[] toBytes();

    /** Opens a new input stream for this resource. */
    SeekableInputStream newInputStream() throws IOException;

    /**
     * Creates a {@link Resource} instance based on the given {@link ResourceType}.
     *
     * @param resourceType the type of resource to create
     * @param identifier the identifier of the resource
     * @param comment optional comment describing the resource
     * @param uri the URI pointing to the resource location
     * @param size the size of the resource in bytes
     * @param lastModifiedTime the last modified time in milliseconds since epoch
     * @param uriReaderFactory factory to read the resource URI
     * @return a concrete {@link Resource} instance
     */
    static Resource toResource(
            ResourceType resourceType,
            Identifier identifier,
            @Nullable String comment,
            String uri,
            long size,
            long lastModifiedTime,
            UriReaderFactory uriReaderFactory) {
        String name = identifier.getObjectName();
        switch (resourceType) {
            case FILE:
                return new FileResource(
                        identifier, comment, uri, size, lastModifiedTime, uriReaderFactory);
            case ARCHIVE:
                return new ArchiveResource(
                        identifier, comment, uri, size, lastModifiedTime, uriReaderFactory);
            case JAR:
                if (!name.endsWith(".jar")) {
                    throw new IllegalArgumentException(
                            "JAR resource name must end with '.jar', but got: " + name);
                }
                return new JarResource(
                        identifier, comment, uri, size, lastModifiedTime, uriReaderFactory);
            case PY:
                if (!name.endsWith(".py")) {
                    throw new IllegalArgumentException(
                            "PY resource name must end with '.py', but got: " + name);
                }
                return new PyResource(
                        identifier, comment, uri, size, lastModifiedTime, uriReaderFactory);
            default:
                throw new IllegalArgumentException("Unknown resource type: " + resourceType);
        }
    }
}
