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

package org.apache.paimon.filter;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utils to check secondary index (e.g. bloom filter) predicate. */
public class PredicateFilterUtil {

    public static boolean checkPredicate(Path path, FileIO fileIO, @Nullable Predicate predicate)
            throws IOException {
        try (DataInputStream dataInput = new DataInputStream(fileIO.newInputStream(path))) {
            return checkPredicate(dataInput, predicate);
        }
    }

    public static boolean checkPredicate(DataInput dataInput, @Nullable Predicate predicate) {
        if (predicate == null) {
            return true;
        }

        Pair<String, Map<String, byte[]>> pair = deserializeIndexString(dataInput);

        String type = pair.getLeft();
        Map<String, byte[]> checker = pair.getRight();

        List<PredicateTester> testers =
                checker.entrySet().stream()
                        .map(
                                entry ->
                                        new PredicateTester(
                                                entry.getKey(),
                                                FilterInterface.getFilter(type)
                                                        .recoverFrom(entry.getValue())))
                        .collect(Collectors.toList());

        for (PredicateTester tester : testers) {
            if (!predicate.visit(tester)) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkPredicate(byte[] serializedBytes, @Nullable Predicate predicate) {
        if (predicate == null) {
            return true;
        }

        Pair<String, Map<String, byte[]>> pair = deserializeIndexString(serializedBytes);

        String type = pair.getLeft();
        Map<String, byte[]> checker = pair.getRight();

        List<PredicateTester> testers =
                checker.entrySet().stream()
                        .map(
                                entry ->
                                        new PredicateTester(
                                                entry.getKey(),
                                                FilterInterface.getFilter(type)
                                                        .recoverFrom(entry.getValue())))
                        .collect(Collectors.toList());

        for (PredicateTester tester : testers) {
            if (!predicate.visit(tester)) {
                return false;
            }
        }
        return true;
    }

    public static byte[] serializeIndexMap(String type, Map<String, byte[]> indexMap) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutput = new DataOutputStream(byteArrayOutputStream);
            byte[] typeBytes = type.getBytes(StandardCharsets.UTF_8);
            dataOutput.writeInt(typeBytes.length);
            dataOutput.write(typeBytes);

            dataOutput.writeInt(indexMap.size());

            for (Map.Entry<String, byte[]> entry : indexMap.entrySet()) {
                byte[] columnName = entry.getKey().getBytes(StandardCharsets.UTF_8);
                dataOutput.writeInt(columnName.length);
                dataOutput.write(columnName);
                dataOutput.writeInt(entry.getValue().length);
                dataOutput.write(entry.getValue());
            }

            byte[] serializedBytes = byteArrayOutputStream.toByteArray();
            dataOutput.close();
            return serializedBytes;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Pair<String, Map<String, byte[]>> deserializeIndexString(byte[] serializedBytes) {
        DataInput dataInput = new DataInputStream(new ByteArrayInputStream(serializedBytes));

        return deserializeIndexString(dataInput);
    }

    public static Pair<String, Map<String, byte[]>> deserializeIndexString(DataInput dataInput) {
        Map<String, byte[]> indexMap = new HashMap<>();
        try {
            int typeLength = dataInput.readInt();
            byte[] typeByte = new byte[typeLength];
            dataInput.readFully(typeByte);
            String type = new String(typeByte, StandardCharsets.UTF_8);

            int size = dataInput.readInt();
            for (int i = 0; i < size; i++) {
                byte[] columnNameByte = new byte[dataInput.readInt()];
                dataInput.readFully(columnNameByte);
                byte[] indexBytes = new byte[dataInput.readInt()];
                dataInput.readFully(indexBytes);
                indexMap.put(new String(columnNameByte, StandardCharsets.UTF_8), indexBytes);
            }

            return Pair.of(type, indexMap);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
