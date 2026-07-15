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

import org.apache.paimon.catalog.Identifier;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A table identifier and its optional outermost read authorization entry point. */
public final class RESTReadVia {

    public static final String MARKER = "$via_";

    private final Identifier identifier;
    private final @Nullable Identifier readVia;

    private RESTReadVia(Identifier identifier, @Nullable Identifier readVia) {
        this.identifier = identifier;
        this.readVia = readVia;
    }

    public static RESTReadVia parse(Identifier identifier) {
        String objectName = identifier.getObjectName();
        int markerPos = objectName.lastIndexOf(MARKER);
        if (markerPos < 0) {
            return new RESTReadVia(identifier, null);
        }

        checkArgument(
                markerPos > 0 && objectName.indexOf(MARKER) == markerPos,
                "Invalid REST read-via identifier: " + identifier);
        String viaName = objectName.substring(markerPos + MARKER.length());
        int databaseSeparator = viaName.indexOf('.');
        checkArgument(
                databaseSeparator > 0
                        && databaseSeparator < viaName.length() - 1
                        && !viaName.substring(0, databaseSeparator).trim().isEmpty()
                        && !viaName.substring(databaseSeparator + 1).trim().isEmpty(),
                "Invalid REST read-via identifier: " + identifier);
        return new RESTReadVia(
                Identifier.create(identifier.getDatabaseName(), objectName.substring(0, markerPos)),
                Identifier.fromString(viaName));
    }

    public static Identifier withReadVia(Identifier identifier, Identifier readVia) {
        if (parse(identifier).readVia().isPresent()) {
            return identifier;
        }
        return Identifier.create(
                identifier.getDatabaseName(),
                identifier.getObjectName() + MARKER + readVia.getFullName());
    }

    public Identifier identifier() {
        return identifier;
    }

    public Optional<Identifier> readVia() {
        return Optional.ofNullable(readVia);
    }
}
