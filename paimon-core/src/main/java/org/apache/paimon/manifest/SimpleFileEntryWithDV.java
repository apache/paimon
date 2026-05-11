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

package org.apache.paimon.manifest;

import javax.annotation.Nullable;

import java.util.Objects;

/** A {@link FileEntry} contains {@link SimpleFileEntry} and dv file name. */
public class SimpleFileEntryWithDV extends SimpleFileEntry {

    @Nullable private final String dvFileName;

    public SimpleFileEntryWithDV(SimpleFileEntry entry, @Nullable String dvFileName) {
        super(
                entry.kind(),
                entry.partition(),
                entry.bucket(),
                entry.totalBuckets(),
                entry.level(),
                entry.fileName(),
                entry.extraFiles(),
                entry.embeddedIndex(),
                entry.minKey(),
                entry.maxKey(),
                entry.externalPath(),
                entry.rowCount(),
                entry.firstRowId());
        this.dvFileName = dvFileName;
    }

    public Identifier identifier() {
        return new IdentifierWithDv(super.identifier(), dvFileName);
    }

    @Nullable
    public String dvFileName() {
        return dvFileName;
    }

    public SimpleFileEntry toDelete() {
        return new SimpleFileEntryWithDV(super.toDelete(), dvFileName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SimpleFileEntryWithDV that = (SimpleFileEntryWithDV) o;
        return Objects.equals(dvFileName, that.dvFileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dvFileName);
    }

    @Override
    public String toString() {
        return super.toString() + ", {dvFileName=" + dvFileName + '}';
    }

    /**
     * The same {@link Identifier} indicates that the {@link ManifestEntry} refers to the same data
     * file.
     */
    static class IdentifierWithDv extends Identifier {

        private final String dvFileName;

        public IdentifierWithDv(Identifier identifier, String dvFileName) {
            super(
                    identifier.partition,
                    identifier.bucket,
                    identifier.level,
                    identifier.fileName,
                    identifier.extraFiles,
                    identifier.embeddedIndex,
                    identifier.externalPath);
            this.dvFileName = dvFileName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            IdentifierWithDv that = (IdentifierWithDv) o;
            return Objects.equals(dvFileName, that.dvFileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), dvFileName);
        }
    }
}
