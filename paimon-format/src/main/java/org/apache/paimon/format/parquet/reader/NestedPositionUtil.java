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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.format.parquet.position.CollectionPosition;
import org.apache.paimon.format.parquet.type.ParquetField;
import org.apache.paimon.utils.BooleanArrayList;
import org.apache.paimon.utils.LongArrayList;

import static java.lang.String.format;

/** Utils to calculate nested type position. */
public class NestedPositionUtil {

    /**
     * Calculate the collection's offsets according to column's max repetition level, definition
     * level, value's repetition level and definition level. Each collection (Array or Map) has four
     * situation:
     * <li>Collection is not defined, because optional parent fields is null, this is decided by its
     *     parent's repetition level
     * <li>Collection is null
     * <li>Collection is defined but empty
     * <li>Collection is defined and not empty. In this case offset value is increased by the number
     *     of elements in that collection
     *
     * @param field field that contains array/map column message include max repetition level and
     *     definition level.
     * @param definitionLevels int array with each value's repetition level.
     * @param repetitionLevels int array with each value's definition level.
     * @return {@link CollectionPosition} contains collections offset array, length array and isNull
     *     array.
     */
    public static CollectionPosition calculateCollectionOffsets(
            ParquetField field,
            int[] definitionLevels,
            int[] repetitionLevels,
            boolean readRowField) {
        int collectionDefinitionLevel = field.getDefinitionLevel();
        int collectionRepetitionLevel = field.getRepetitionLevel() + 1;
        int offset = 0;
        int valueCount = 0;
        LongArrayList offsets = new LongArrayList(0);
        offsets.add(offset);
        BooleanArrayList emptyCollectionFlags = new BooleanArrayList(0);
        BooleanArrayList nullCollectionFlags = new BooleanArrayList(0);
        int nullValuesCount = 0;
        for (int i = 0;
                i < definitionLevels.length;
                i = getNextCollectionStartIndex(repetitionLevels, collectionRepetitionLevel, i)) {
            if (definitionLevels[i] >= collectionDefinitionLevel - 1) {
                // definitionLevels[i] > collectionDefinitionLevel  => Collection is defined and not
                // empty
                // definitionLevels[i] == collectionDefinitionLevel => Collection is defined but
                // empty
                // definitionLevels[i] == collectionDefinitionLevel - 1 => Collection is defined but
                // null
                if (definitionLevels[i] > collectionDefinitionLevel) {
                    nullCollectionFlags.add(false);
                    emptyCollectionFlags.add(false);
                    offset += getCollectionSize(repetitionLevels, collectionRepetitionLevel, i + 1);
                } else if (definitionLevels[i] == collectionDefinitionLevel) {
                    nullCollectionFlags.add(false);
                    emptyCollectionFlags.add(true);
                    // don't increase offset for empty values
                } else {
                    nullCollectionFlags.add(true);
                    nullValuesCount++;
                    // 1. don't increase offset for null values
                    // 2. offsets and emptyCollectionFlags are meaningless for null values, but they
                    // must be set at each index for calculating lengths later
                    emptyCollectionFlags.add(false);
                }
                offsets.add(offset);
                valueCount++;
            } else if (definitionLevels[i] == collectionDefinitionLevel - 2 && readRowField) {
                // row field should store null value
                nullCollectionFlags.add(true);
                nullValuesCount++;
                emptyCollectionFlags.add(false);

                offsets.add(offset);
                valueCount++;
            }
            // else when definitionLevels[i] < collectionDefinitionLevel - 1, it means the
            // collection is not defined, just ignore it
        }
        long[] offsetsArray = offsets.toArray();
        long[] length = calculateLengthByOffsets(emptyCollectionFlags.toArray(), offsetsArray);
        if (nullValuesCount == 0) {
            return new CollectionPosition(null, offsetsArray, length, valueCount);
        }
        return new CollectionPosition(
                nullCollectionFlags.toArray(), offsetsArray, length, valueCount);
    }

    public static long[] calculateLengthByOffsets(
            boolean[] collectionIsEmpty, long[] arrayOffsets) {
        LongArrayList lengthList = new LongArrayList(arrayOffsets.length);
        for (int i = 0; i < arrayOffsets.length - 1; i++) {
            long offset = arrayOffsets[i];
            long length = arrayOffsets[i + 1] - offset;
            if (length < 0) {
                throw new IllegalArgumentException(
                        format(
                                "Offset is not monotonically ascending. offsets[%s]=%s, offsets[%s]=%s",
                                i, arrayOffsets[i], i + 1, arrayOffsets[i + 1]));
            }
            if (collectionIsEmpty[i]) {
                length = 0;
            }
            lengthList.add(length);
        }
        return lengthList.toArray();
    }

    private static int getNextCollectionStartIndex(
            int[] repetitionLevels, int maxRepetitionLevel, int elementIndex) {
        do {
            elementIndex++;
        } while (hasMoreElements(repetitionLevels, elementIndex)
                && isNotCollectionBeginningMarker(
                        repetitionLevels, maxRepetitionLevel, elementIndex));
        return elementIndex;
    }

    /** This method is only called for non-empty collections. */
    private static int getCollectionSize(
            int[] repetitionLevels, int maxRepetitionLevel, int nextIndex) {
        int size = 1;
        while (hasMoreElements(repetitionLevels, nextIndex)
                && isNotCollectionBeginningMarker(
                        repetitionLevels, maxRepetitionLevel, nextIndex)) {
            // Collection elements cannot only be primitive, but also can have nested structure
            // Counting only elements which belong to current collection, skipping inner elements of
            // nested collections/structs
            if (repetitionLevels[nextIndex] <= maxRepetitionLevel) {
                size++;
            }
            nextIndex++;
        }
        return size;
    }

    private static boolean isNotCollectionBeginningMarker(
            int[] repetitionLevels, int maxRepetitionLevel, int nextIndex) {
        return repetitionLevels[nextIndex] >= maxRepetitionLevel;
    }

    private static boolean hasMoreElements(int[] repetitionLevels, int nextIndex) {
        return nextIndex < repetitionLevels.length;
    }
}
