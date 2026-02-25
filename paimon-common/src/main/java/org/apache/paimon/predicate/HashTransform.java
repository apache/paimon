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

import org.apache.paimon.shade.guava30.com.google.common.hash.HashCode;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link Transform} which masks a string column by hashing it.
 *
 * <p>Output is a hex string. Default algorithm is {@code SHA-256}.
 */
public class HashTransform extends StringTransform {

    private static final long serialVersionUID = 1L;

    private final String algorithm;

    @Nullable private final BinaryString salt;

    private transient MessageDigest digest;

    public HashTransform(FieldRef fieldRef) {
        this(fieldRef, null, null);
    }

    public HashTransform(
            FieldRef fieldRef, @Nullable String algorithm, @Nullable BinaryString salt) {
        super(Arrays.asList(Objects.requireNonNull(fieldRef, "fieldRef must not be null")));
        this.algorithm = resolveAlgorithm(algorithm);
        this.salt = salt;
        ensureDigest();
    }

    public FieldRef fieldRef() {
        return (FieldRef) inputs().get(0);
    }

    public String algorithm() {
        return algorithm;
    }

    @Nullable
    public BinaryString salt() {
        return salt;
    }

    @Nullable
    @Override
    protected BinaryString transform(List<BinaryString> inputs) {
        BinaryString s = inputs.get(0);
        if (s == null) {
            return null;
        }

        MessageDigest md = ensureDigest();
        md.reset();

        if (salt != null) {
            md.update(salt.toString().getBytes(StandardCharsets.UTF_8));
        }
        md.update(s.toString().getBytes(StandardCharsets.UTF_8));

        return BinaryString.fromString(HashCode.fromBytes(md.digest()).toString());
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        List<Object> nonNullInputs =
                Objects.requireNonNull(inputs, "HashTransform expects non-null inputs");
        checkArgument(nonNullInputs.size() == 1, "HashTransform expects 1 input");
        checkArgument(
                nonNullInputs.get(0) instanceof FieldRef, "HashTransform input must be FieldRef");
        return new HashTransform((FieldRef) nonNullInputs.get(0), algorithm, salt);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HashTransform that = (HashTransform) o;
        return Objects.equals(fieldRef(), that.fieldRef())
                && Objects.equals(algorithm, that.algorithm)
                && Objects.equals(salt, that.salt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldRef(), algorithm, salt);
    }

    @Override
    public String toString() {
        return "HashTransform{"
                + "fieldRef="
                + fieldRef()
                + ", algorithm='"
                + algorithm
                + '\''
                + ", salt="
                + salt
                + '}';
    }

    private MessageDigest ensureDigest() {
        if (digest == null) {
            try {
                digest = MessageDigest.getInstance(algorithm);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalArgumentException("Unsupported hash algorithm: " + algorithm, e);
            }
        }
        return digest;
    }

    private static String resolveAlgorithm(@Nullable String algorithm) {
        if (algorithm == null || algorithm.trim().isEmpty()) {
            return "SHA-256";
        }
        String a = algorithm.trim();
        String normalized = a.replace("-", "").toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "md5":
                return "MD5";
            case "sha1":
                return "SHA-1";
            case "sha256":
                return "SHA-256";
            case "sha512":
                return "SHA-512";
            default:
                return a;
        }
    }
}
