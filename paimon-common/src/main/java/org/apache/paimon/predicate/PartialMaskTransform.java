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

package org.apache.paimon.predicate;

import org.apache.paimon.data.BinaryString;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link Transform} which masks the middle part of a string by keeping the specified number of
 * prefix and suffix characters.
 *
 * <p>If the string length is less than or equal to {@code prefixLen + suffixLen}, the output will
 * be a fully masked string of the same length.
 */
public class PartialMaskTransform extends StringTransform {

    private static final long serialVersionUID = 1L;

    private final int prefixLen;
    private final int suffixLen;

    public PartialMaskTransform(
            FieldRef fieldRef, int prefixLen, int suffixLen, BinaryString mask) {
        super(
                Arrays.asList(
                        Objects.requireNonNull(fieldRef, "fieldRef must not be null"),
                        Objects.requireNonNull(mask, "mask must not be null")));
        checkArgument(prefixLen >= 0, "prefixLen must be >= 0");
        checkArgument(suffixLen >= 0, "suffixLen must be >= 0");
        // "mask" is treated as a token repeated by character count. Empty mask would be invalid.
        checkArgument(mask.numChars() > 0, "mask must not be empty");

        this.prefixLen = prefixLen;
        this.suffixLen = suffixLen;
    }

    public FieldRef fieldRef() {
        return (FieldRef) inputs().get(0);
    }

    public int prefixLen() {
        return prefixLen;
    }

    public int suffixLen() {
        return suffixLen;
    }

    public BinaryString mask() {
        return (BinaryString) inputs().get(1);
    }

    @Nullable
    @Override
    protected BinaryString transform(List<BinaryString> inputs) {
        BinaryString s = inputs.get(0);
        if (s == null) {
            return null;
        }
        BinaryString mask = inputs.get(1);
        checkArgument(mask != null, "mask must not be null");

        int len = s.numChars();
        if (len <= 0) {
            return s;
        }

        if (prefixLen + suffixLen >= len) {
            return repeat(mask, len);
        }

        BinaryString prefix = prefixLen == 0 ? BinaryString.EMPTY_UTF8 : s.substring(0, prefixLen);
        BinaryString suffix =
                suffixLen == 0 ? BinaryString.EMPTY_UTF8 : s.substring(len - suffixLen, len);
        int middleLen = len - prefixLen - suffixLen;
        BinaryString middle = middleLen == 0 ? BinaryString.EMPTY_UTF8 : repeat(mask, middleLen);
        return BinaryString.concat(prefix, middle, suffix);
    }

    private static BinaryString repeat(BinaryString token, int times) {
        if (times <= 0) {
            return BinaryString.EMPTY_UTF8;
        }
        String t = token.toString();
        StringBuilder sb = new StringBuilder(t.length() * times);
        for (int i = 0; i < times; i++) {
            sb.append(t);
        }
        return BinaryString.fromString(sb.toString());
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        List<Object> nonNullInputs =
                Objects.requireNonNull(inputs, "PartialMaskTransform expects non-null inputs");
        checkArgument(nonNullInputs.size() == 2, "PartialMaskTransform expects 2 inputs");
        checkArgument(
                nonNullInputs.get(0) instanceof FieldRef,
                "PartialMaskTransform input must be FieldRef");
        checkArgument(
                nonNullInputs.get(1) instanceof BinaryString,
                "PartialMaskTransform mask input must be BinaryString");
        return new PartialMaskTransform(
                (FieldRef) nonNullInputs.get(0),
                prefixLen,
                suffixLen,
                (BinaryString) nonNullInputs.get(1));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartialMaskTransform that = (PartialMaskTransform) o;
        return prefixLen == that.prefixLen
                && suffixLen == that.suffixLen
                && Objects.equals(fieldRef(), that.fieldRef())
                && Objects.equals(mask(), that.mask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldRef(), prefixLen, suffixLen, mask());
    }

    @Override
    public String toString() {
        return "PartialMaskTransform{"
                + "fieldRef="
                + fieldRef()
                + ", prefixLen="
                + prefixLen
                + ", suffixLen="
                + suffixLen
                + ", mask="
                + mask()
                + '}';
    }
}
