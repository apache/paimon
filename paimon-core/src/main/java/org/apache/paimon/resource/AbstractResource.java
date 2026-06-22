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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/** Abstract base implementation of {@link Resource} with common fields and accessors. */
public abstract class AbstractResource implements Resource {

    private static final long serialVersionUID = 1L;

    private final Identifier identifier;
    @Nullable private final String comment;
    private final String uri;
    private final long size;
    private final long lastModifiedTime;
    private final FileIO fileIO;

    protected AbstractResource(
            Identifier identifier,
            @Nullable String comment,
            String uri,
            long size,
            long lastModifiedTime,
            FileIO fileIO) {
        this.identifier = identifier;
        this.comment = comment;
        this.uri = uri;
        this.size = size;
        this.lastModifiedTime = lastModifiedTime;
        this.fileIO = fileIO;
    }

    @JsonGetter("name")
    @Override
    public String name() {
        return identifier.getObjectName();
    }

    @JsonGetter("fullName")
    @Override
    public String fullName() {
        return identifier.getFullName();
    }

    @JsonIgnore
    public Identifier identifier() {
        return identifier;
    }

    @JsonGetter("comment")
    public @Nullable String commentOrNull() {
        return comment;
    }

    @Override
    public Optional<String> comment() {
        return Optional.ofNullable(comment);
    }

    @JsonGetter("uri")
    @Override
    public String uri() {
        return uri;
    }

    @JsonGetter("size")
    @Override
    public long size() {
        return size;
    }

    @JsonGetter("lastModifiedTime")
    @Override
    public long lastModifiedTime() {
        return lastModifiedTime;
    }

    @JsonGetter("resourceType")
    public String resourceTypeValue() {
        return resourceType().getValue();
    }

    @Override
    public byte[] toBytes() {
        try {
            return IOUtils.readFully(newInputStream(), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SeekableInputStream newInputStream() throws IOException {
        return fileIO.newInputStream(new Path(uri));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractResource that = (AbstractResource) o;
        return size == that.size
                && lastModifiedTime == that.lastModifiedTime
                && Objects.equals(identifier, that.identifier)
                && Objects.equals(comment, that.comment)
                && Objects.equals(uri, that.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, comment, uri, size, lastModifiedTime);
    }
}
