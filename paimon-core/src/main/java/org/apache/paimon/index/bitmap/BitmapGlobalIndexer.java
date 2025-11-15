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

package org.apache.paimon.index.bitmap;

import org.apache.paimon.fileindex.bitmap.BitmapFileIndexMeta;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexMetaV2;
import org.apache.paimon.fileindex.bitmap.BitmapReaderHelper;
import org.apache.paimon.fileindex.bitmap.BitmapWriterHelper;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.globalindex.GlobaIndexBuilder;
import org.apache.paimon.index.globalindex.GlobalIndexFileHelper;
import org.apache.paimon.index.globalindex.GlobalIndexLeafPredicator;
import org.apache.paimon.index.globalindex.GlobalIndexResult;
import org.apache.paimon.index.globalindex.GlobalIndexer;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RoaringBitmap64;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** The implementation of bitmap global index. */
public class BitmapGlobalIndexer extends GlobalIndexer {

    public static final int VERSION_2 = 2;
    public static final String VERSION = "version";

    private final DataType dataType;
    private final Options options;

    public BitmapGlobalIndexer(DataType dataType, Options options) {
        this.dataType = dataType;
        this.options = options;
    }

    @Override
    public GlobaIndexBuilder createBuilder(GlobalIndexFileHelper globalIndexFileHelper) {
        return new Writer(globalIndexFileHelper, dataType, options);
    }

    @Override
    public GlobalIndexLeafPredicator createReader(
            GlobalIndexFileHelper globalIndexFileInputHelper, List<IndexFileMeta> metas)
            throws IOException {
        IndexFileMeta indexFileMeta = metas.get(0);
        SeekableInputStream seekableInputStream =
                globalIndexFileInputHelper.getInputStream(indexFileMeta);
        return new Reader(seekableInputStream, 0, options);
    }

    private static class Writer extends GlobaIndexBuilder {

        private final BitmapWriterHelper<RoaringBitmap64> helper;
        private int rowCount = 0;

        public Writer(
                GlobalIndexFileHelper globalIndexFileHelper, DataType dataType, Options options) {
            super(globalIndexFileHelper);
            int version = options.getInteger(VERSION, VERSION_2);
            this.helper =
                    new BitmapWriterHelper<>(
                            version,
                            dataType,
                            options,
                            new BitmapWriterHelper.Bitmap64Operations());
        }

        @Override
        public void indexTo(Object key, long rowId) {
            rowCount++;
            helper.add(key, rowId);
        }

        @Override
        public List<Pair<String, byte[]>> end() {
            String outputFileName = globalIndexFileHelper.newFileName("bitmap");
            try (DataOutputStream dos =
                    new DataOutputStream(globalIndexFileHelper.newOutputStream(outputFileName))) {
                helper.serialize(dos, rowCount);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return Collections.singletonList(Pair.of(outputFileName, null));
        }
    }

    private static class Reader extends GlobalIndexLeafPredicator {

        private final BitmapReaderHelper<RoaringBitmap64> helper;

        public Reader(SeekableInputStream seekableInputStream, int start, Options options) {
            this.helper =
                    new BitmapReaderHelper<>(
                            seekableInputStream,
                            start,
                            options,
                            new BitmapReaderHelper.Bitmap64Operations());
        }

        @Override
        public GlobalIndexResult visitEqual(FieldRef fieldRef, Object literal) {
            return visitIn(fieldRef, Collections.singletonList(literal));
        }

        @Override
        public GlobalIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
            return visitNotIn(fieldRef, Collections.singletonList(literal));
        }

        @Override
        public GlobalIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
            return new GlobalBitmapIndexResult(
                    () -> {
                        helper.readInternalMeta(fieldRef.type(), this::createBitmapFileIndexMeta);
                        return helper.getInListResultBitmap(literals);
                    });
        }

        @Override
        public GlobalIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return new GlobalBitmapIndexResult(
                    () -> {
                        helper.readInternalMeta(fieldRef.type(), this::createBitmapFileIndexMeta);
                        RoaringBitmap64 bitmap = helper.getInListResultBitmap(literals);
                        bitmap.flip(0, helper.getBitmapFileIndexMeta().getRowCount());
                        return bitmap;
                    });
        }

        @Override
        public GlobalIndexResult visitIsNull(FieldRef fieldRef) {
            return visitIn(fieldRef, Collections.singletonList(null));
        }

        @Override
        public GlobalIndexResult visitIsNotNull(FieldRef fieldRef) {
            return visitNotIn(fieldRef, Collections.singletonList(null));
        }

        @Override
        public void close() throws IOException {
            helper.close();
        }

        private BitmapFileIndexMeta createBitmapFileIndexMeta(
                int version, DataType dataType, Options options) {
            checkArgument(
                    version == VERSION_2,
                    "Only version 2 bitmap file index is supported, but found %s",
                    version);
            return new BitmapFileIndexMetaV2(dataType, options);
        }
    }
}
