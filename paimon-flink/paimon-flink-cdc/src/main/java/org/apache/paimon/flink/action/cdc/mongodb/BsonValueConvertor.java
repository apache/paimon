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

package org.apache.paimon.flink.action.cdc.mongodb;

import org.apache.paimon.utils.Pair;

import org.apache.flink.util.CollectionUtil;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code BsonValueConvertor} class is designed to convert {@link BsonValue} to Java objects.
 */
public class BsonValueConvertor {
    private static Integer convert(BsonTimestamp bsonTimestamp) {
        return bsonTimestamp.getTime();
    }

    private static BigDecimal convert(BsonDecimal128 bsonValue) {
        return convert(bsonValue.decimal128Value());
    }

    private static BigDecimal convert(Decimal128 bsonValue) {
        if (bsonValue.isNaN() || bsonValue.isInfinite()) {
            return null;
        }
        return bsonValue.bigDecimalValue();
    }

    private static String convert(BsonObjectId objectId) {
        return convert(objectId.getValue());
    }

    private static String convert(ObjectId objectId) {
        return objectId.toHexString();
    }

    private static String convert(BsonBinary bsonBinary) {
        if (BsonBinarySubType.isUuid(bsonBinary.getType())) {
            return bsonBinary.asUuid().toString();
        } else {
            return toHex(bsonBinary.getData());
        }
    }

    private static String convert(BsonUndefined bsonUndefined) {
        return null;
    }

    private static String convert(BsonRegularExpression regex) {
        String escaped =
                regex.getPattern().isEmpty() ? "(?:)" : regex.getPattern().replace("/", "\\/");
        return String.format("/%s/%s", escaped, regex.getOptions());
    }

    private static Double convert(BsonDouble bsonDouble) {
        double value = bsonDouble.getValue();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        }
        return value;
    }

    private static String convert(BsonString string) {
        return string.getValue();
    }

    private static Integer convert(BsonInt32 int32) {
        return int32.getValue();
    }

    private static Long convert(BsonInt64 int64) {
        return int64.getValue();
    }

    private static Boolean convert(BsonBoolean bool) {
        return bool.getValue();
    }

    private static Long convert(BsonDateTime dateTime) {
        return dateTime.getValue();
    }

    private static String convert(BsonSymbol symbol) {
        return symbol.getSymbol();
    }

    private static String convert(BsonJavaScript javascript) {
        return javascript.getCode();
    }

    private static Map<String, Object> convert(BsonJavaScriptWithScope javascriptWithScope) {
        return CollectionUtil.map(
                Pair.of("code", javascriptWithScope.getCode()),
                Pair.of("scope", convert(javascriptWithScope.getScope())));
    }

    private static String convert(BsonNull bsonNull) {
        return null;
    }

    private static String convert(BsonDbPointer dbPointer) {
        return dbPointer.toString();
    }

    private static String convert(BsonMaxKey maxKey) {
        return maxKey.toString();
    }

    private static String convert(BsonMinKey minKey) {
        return minKey.toString();
    }

    private static Map<String, Object> convert(BsonDocument document) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>(document.size());
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            map.put(entry.getKey(), convert(entry.getValue()));
        }
        return map;
    }

    private static List<Object> convert(BsonArray array) {
        ArrayList<Object> objects = new ArrayList<>(array.size());
        for (BsonValue bsonValue : array) {
            objects.add(convert(bsonValue));
        }
        return objects;
    }

    public static Object convert(BsonValue bsonValue) {
        if (bsonValue == null) {
            return null;
        }
        switch (bsonValue.getBsonType()) {
            case TIMESTAMP:
                return convert(bsonValue.asTimestamp());
            case DECIMAL128:
                return convert(bsonValue.asDecimal128());
            case OBJECT_ID:
                return convert(bsonValue.asObjectId());
            case BINARY:
                return convert(bsonValue.asBinary());
            case UNDEFINED:
                return convert((BsonUndefined) bsonValue);
            case REGULAR_EXPRESSION:
                return convert(bsonValue.asRegularExpression());
            case DOUBLE:
                return convert(bsonValue.asDouble());
            case STRING:
                return convert(bsonValue.asString());
            case INT32:
                return convert(bsonValue.asInt32());
            case INT64:
                return convert(bsonValue.asInt64());
            case BOOLEAN:
                return convert(bsonValue.asBoolean());
            case DATE_TIME:
                return convert(bsonValue.asDateTime());
            case SYMBOL:
                return convert(bsonValue.asSymbol());
            case JAVASCRIPT:
                return convert(bsonValue.asJavaScript());
            case JAVASCRIPT_WITH_SCOPE:
                return convert(bsonValue.asJavaScriptWithScope());
            case NULL:
                return convert((BsonNull) bsonValue);
            case DB_POINTER:
                return convert(bsonValue.asDBPointer());
            case MAX_KEY:
                return convert((BsonMaxKey) bsonValue);
            case MIN_KEY:
                return convert((BsonMinKey) bsonValue);
            case DOCUMENT:
                return convert(bsonValue.asDocument());
            case ARRAY:
                return convert(bsonValue.asArray());
            default:
                throw new IllegalArgumentException(
                        "Unsupported BSON type: " + bsonValue.getBsonType());
        }
    }

    public static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();

        for (byte b : bytes) {
            String s = Integer.toHexString(255 & b);
            if (s.length() < 2) {
                sb.append("0");
            }

            sb.append(s);
        }

        return sb.toString();
    }
}
