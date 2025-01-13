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

package org.apache.paimon.flink.action.cdc.format.aliyun;

/** Converts some special types such as enum、set、geometry. */
public class AliyunFieldParser {

    protected static byte[] convertGeoType2WkbArray(byte[] mysqlGeomBytes) {
        int sridLength = 4;
        boolean hasSrid = false;
        for (int i = 0; i < sridLength; ++i) {
            if (mysqlGeomBytes[i] != 0) {
                hasSrid = true;
                break;
            }
        }
        byte[] wkb;
        if (hasSrid) {
            wkb = new byte[mysqlGeomBytes.length];
            // byteOrder + geometry
            System.arraycopy(mysqlGeomBytes, 4, wkb, 0, 5);
            // srid
            System.arraycopy(mysqlGeomBytes, 0, wkb, 5, 4);
            // geometry
            System.arraycopy(mysqlGeomBytes, 9, wkb, 9, wkb.length - 9);

            // set srid flag
            if (wkb[0] == 0) {
                // big endian
                wkb[1] = (byte) (wkb[1] + 32);
            } else {
                wkb[4] = (byte) (wkb[4] + 32);
            }
        } else {
            wkb = new byte[mysqlGeomBytes.length - 4];
            System.arraycopy(mysqlGeomBytes, 4, wkb, 0, wkb.length);
        }
        return wkb;
    }

    protected static String convertSet(String value, String mysqlType) {
        // mysql set type value can be filled with more than one, value is a bit string conversion
        // from the long
        int indexes = Integer.parseInt(value);
        return getSetValuesByIndex(mysqlType, indexes);
    }

    protected static String convertEnum(String value, String mysqlType) {
        int elementIndex = Integer.parseInt(value);
        // enum('a','b','c')
        return getEnumValueByIndex(mysqlType, elementIndex);
    }

    protected static String getEnumValueByIndex(String mysqlType, int elementIndex) {
        String[] options = extractEnumValueByIndex(mysqlType);

        return options[elementIndex - 1];
    }

    protected static String getSetValuesByIndex(String mysqlType, int indexes) {
        String[] options = extractSetValuesByIndex(mysqlType);

        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int index = 0;
        boolean first = true;
        int optionLen = options.length;

        while (indexes != 0L) {
            if (indexes % 2L != 0) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }
                if (index < optionLen) {
                    sb.append(options[index]);
                } else {
                    throw new RuntimeException(
                            String.format(
                                    "extractSetValues from mysqlType[%s],index:%d failed",
                                    mysqlType, indexes));
                }
            }
            ++index;
            indexes = indexes >>> 1;
        }
        sb.append("]");
        return sb.toString();
    }

    private static String[] extractSetValuesByIndex(String mysqlType) {
        // set('x','y')
        return mysqlType.substring(5, mysqlType.length() - 2).split("'\\s*,\\s*'");
    }

    private static String[] extractEnumValueByIndex(String mysqlType) {
        // enum('x','y')
        return mysqlType.substring(6, mysqlType.length() - 2).split("'\\s*,\\s*'");
    }
}
