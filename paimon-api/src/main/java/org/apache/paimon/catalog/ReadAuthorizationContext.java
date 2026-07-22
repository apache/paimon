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

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.Public;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Authorization context which lazily obtains grants for discovered dependency resources. */
@Public
public final class ReadAuthorizationContext implements Serializable {

    private static final long serialVersionUID = 4L;

    private static final ReadAuthorizationContext DIRECT =
            new ReadAuthorizationContext(null, GrantState.empty());

    @Nullable private final ReadAuthorizationResource authorizationRoot;
    private volatile GrantState grantState;

    private ReadAuthorizationContext(
            @Nullable ReadAuthorizationResource authorizationRoot, GrantState grantState) {
        this.authorizationRoot = authorizationRoot;
        this.grantState = Objects.requireNonNull(grantState, "grantState");
    }

    /** Returns a context for reading a resource directly. */
    public static ReadAuthorizationContext direct() {
        return DIRECT;
    }

    /** Returns a pending context for dependencies discovered while reading a view. */
    public static ReadAuthorizationContext forView(Identifier authorizationRoot) {
        return forRoot(ReadAuthorizationResource.view(authorizationRoot));
    }

    /** Returns a pending context for dependencies discovered while reading a table. */
    public static ReadAuthorizationContext forTable(Identifier authorizationRoot) {
        return forRoot(ReadAuthorizationResource.table(authorizationRoot));
    }

    private static ReadAuthorizationContext forRoot(ReadAuthorizationResource root) {
        Objects.requireNonNull(root, "root");
        return new ReadAuthorizationContext(root, GrantState.empty());
    }

    /** Returns a context initialized from a server-issued response. */
    public static ReadAuthorizationContext granted(
            ReadAuthorizationRootType authorizationRootType,
            Identifier authorizationRoot,
            List<ReadAuthorizationResource> authorizedResources,
            String readGrant,
            long expiresAtMillis) {
        ReadAuthorizationContext context =
                forRoot(new ReadAuthorizationResource(authorizationRootType, authorizationRoot));
        context.updateGrant(authorizedResources, readGrant, expiresAtMillis);
        return context;
    }

    public boolean isDirect() {
        return authorizationRoot == null;
    }

    /** Returns the type of the outermost resource which caused dependencies to be read. */
    public Optional<ReadAuthorizationRootType> authorizationRootType() {
        return authorizationRoot == null ? Optional.empty() : Optional.of(authorizationRoot.type());
    }

    /** Returns the outermost resource which caused dependencies to be read. */
    public Optional<Identifier> authorizationRoot() {
        return authorizationRoot == null
                ? Optional.empty()
                : Optional.of(authorizationRoot.identifier());
    }

    /**
     * Derive a context for dependencies discovered inside an already authorized resource.
     *
     * <p>The outermost root and current signed grant are preserved. The returned context has an
     * independent grant snapshot so parallel dependency branches cannot overwrite each other.
     */
    public ReadAuthorizationContext forDependenciesOf(
            ReadAuthorizationRootType type, Identifier identifier) {
        if (isDirect()) {
            throw new IllegalStateException(
                    "A direct read context has no dependency authorization root");
        }
        ReadAuthorizationResource resource = new ReadAuthorizationResource(type, identifier);
        if (!resource.equals(authorizationRoot) && !authorizes(resource)) {
            throw new SecurityException(
                    "Cannot enter a resource which is not authorized by the current read grant");
        }
        return new ReadAuthorizationContext(authorizationRoot, grantState);
    }

    /** Returns the dependency resources approved by the current grant. */
    public List<ReadAuthorizationResource> authorizedResources() {
        return grantState.authorizedResources;
    }

    /** Returns whether the current server-issued grant reaches {@code resource}. */
    public boolean authorizes(ReadAuthorizationResource resource) {
        Objects.requireNonNull(resource, "resource");
        return grantState.authorizedResources.contains(resource);
    }

    /** Returns whether the current server-issued grant reaches the exact typed resource. */
    public boolean authorizes(ReadAuthorizationRootType type, Identifier identifier) {
        return authorizes(new ReadAuthorizationResource(type, identifier));
    }

    /** Returns the opaque server-issued bearer credential. */
    public Optional<String> readGrant() {
        return Optional.ofNullable(grantState.readGrant);
    }

    public long expiresAtMillis() {
        return grantState.expiresAtMillis;
    }

    /**
     * Compare the server timestamp for diagnostics only. REST authorization must use the server's
     * clock; clients must not reject a request solely from this local comparison.
     */
    public boolean isExpired(long nowMillis) {
        return !isDirect()
                && (grantState.readGrant == null || nowMillis >= grantState.expiresAtMillis);
    }

    /** Install a server-issued grant and merge the dependency targets accepted by its request. */
    public synchronized void updateGrant(
            List<ReadAuthorizationResource> additionalResources,
            String updatedReadGrant,
            long updatedExpiresAtMillis) {
        if (isDirect()) {
            throw new IllegalStateException("A direct read context cannot contain a grant");
        }
        if (updatedExpiresAtMillis <= 0) {
            throw new IllegalArgumentException("expiresAtMillis must be positive");
        }
        List<ReadAuthorizationResource> validatedResources = validateResources(additionalResources);

        GrantState previous = grantState;
        if (previous.readGrant == null && validatedResources.isEmpty()) {
            throw new IllegalArgumentException(
                    "An initial read grant must authorize at least one dependency resource");
        }
        List<ReadAuthorizationResource> mergedResources =
                new ArrayList<>(previous.authorizedResources);
        for (ReadAuthorizationResource resource : validatedResources) {
            if (!mergedResources.contains(resource)) {
                mergedResources.add(resource);
            }
        }

        this.grantState =
                new GrantState(
                        Collections.unmodifiableList(mergedResources),
                        Objects.requireNonNull(updatedReadGrant, "updatedReadGrant"),
                        updatedExpiresAtMillis);
    }

    private static List<ReadAuthorizationResource> validateResources(
            List<ReadAuthorizationResource> resources) {
        Objects.requireNonNull(resources, "resources");
        List<ReadAuthorizationResource> validated = new ArrayList<>(resources.size());
        for (ReadAuthorizationResource resource : resources) {
            validateResource(Objects.requireNonNull(resource, "resource"));
            if (validated.contains(resource)) {
                throw new IllegalArgumentException(
                        "Read-grant dependency resources must not contain duplicates");
            }
            validated.add(resource);
        }
        return validated;
    }

    private static void validateResource(ReadAuthorizationResource resource) {
        Objects.requireNonNull(resource, "resource");
        Identifier identifier = resource.identifier();
        if (identifier.getDatabaseName() == null
                || identifier.getDatabaseName().isEmpty()
                || Identifier.UNKNOWN_DATABASE.equals(identifier.getDatabaseName())
                || identifier.getObjectName() == null
                || identifier.getObjectName().isEmpty()) {
            throw new IllegalArgumentException(
                    "Read-grant resources must contain an exact database and object name");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReadAuthorizationContext that = (ReadAuthorizationContext) o;
        return Objects.equals(authorizationRoot, that.authorizationRoot)
                && Objects.equals(grantState, that.grantState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authorizationRoot, grantState);
    }

    @Override
    public String toString() {
        GrantState state = grantState;
        return isDirect()
                ? "ReadAuthorizationContext{direct}"
                : String.format(
                        "ReadAuthorizationContext{authorizationRoot=%s, authorizedResources=%s, expiresAtMillis=%s}",
                        authorizationRoot, state.authorizedResources, state.expiresAtMillis);
    }

    /** Immutable holder so readers observe grant updates atomically. */
    private static final class GrantState implements Serializable {

        private static final long serialVersionUID = 1L;

        private final List<ReadAuthorizationResource> authorizedResources;
        @Nullable private final String readGrant;
        private final long expiresAtMillis;

        private GrantState(
                List<ReadAuthorizationResource> authorizedResources,
                @Nullable String readGrant,
                long expiresAtMillis) {
            this.authorizedResources =
                    Collections.unmodifiableList(new ArrayList<>(authorizedResources));
            this.readGrant = readGrant;
            this.expiresAtMillis = expiresAtMillis;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GrantState that = (GrantState) o;
            return expiresAtMillis == that.expiresAtMillis
                    && authorizedResources.equals(that.authorizedResources)
                    && Objects.equals(readGrant, that.readGrant);
        }

        @Override
        public int hashCode() {
            return Objects.hash(authorizedResources, readGrant, expiresAtMillis);
        }

        private static GrantState empty() {
            return new GrantState(Collections.<ReadAuthorizationResource>emptyList(), null, 0L);
        }
    }
}
