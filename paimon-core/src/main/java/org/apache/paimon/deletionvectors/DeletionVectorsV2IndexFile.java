///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.paimon.deletionvectors;
//
//import org.apache.paimon.fs.FileIO;
//import org.apache.paimon.index.IndexFile;
//import org.apache.paimon.index.IndexFileMeta;
//import org.apache.paimon.options.MemorySize;
//import org.apache.paimon.utils.PathFactory;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//
///** doc. */
//public class DeletionVectorsV2IndexFile extends IndexFile {
//    public static final String DELETION_VECTORS_INDEX_V2 = "DELETION_VECTORS_V2";
//    public static final byte VERSION_ID_V2 = 2;
//
//    private final MemorySize targetSizePerIndexFile;
//
//    public DeletionVectorsV2IndexFile(
//            FileIO fileIO, PathFactory pathFactory, MemorySize targetSizePerIndexFile) {
//        super(fileIO, pathFactory);
//        this.targetSizePerIndexFile = targetSizePerIndexFile;
//    }
//
//    public List<IndexFileMeta> write(Map<String, DeletionVector> input) {
//        try {
//            DeletionVectorIndexFileWriter writer =
//                    new DeletionVectorIndexFileWriter(
//                            this.fileIO,
//                            this.pathFactory,
//                            this.targetSizePerIndexFile,
//                            VERSION_ID_V2);
//            return writer.write(input);
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to write deletion vectors.", e);
//        }
//    }
//}
