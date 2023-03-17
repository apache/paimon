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

package org.apache.paimon.annotation;

import org.apache.paimon.options.ConfigOption;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Collection of annotations to modify the behavior of the documentation generators. */
public final class Documentation {

    /** Annotation used on config option fields to override the documented default. */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface OverrideDefault {
        String value();
    }

    /** Annotation used on {@link ConfigOption} fields to exclude it from schema change. */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Immutable {}

    /**
     * Annotation used on config option fields or REST API message headers to exclude it from
     * documentation.
     */
    @Target({ElementType.FIELD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface ExcludeFromDocumentation {
        /** The optional reason why it is excluded from documentation. */
        String value() default "";
    }

    /**
     * Annotation used on config option fields to include them in specific sections. Sections are
     * groups of options that are aggregated across option classes, with each group being placed
     * into a dedicated file.
     *
     * <p>The {@link Documentation.Section#position()} argument controls the position in the
     * generated table, with lower values being placed at the top. Fields with the same position are
     * sorted alphabetically by key.
     */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Section {

        /** The sections in the config docs where this option should be included. */
        String[] value() default {};

        /** The relative position of the option in its section. */
        int position() default Integer.MAX_VALUE;
    }

    /**
     * Annotation used on config option fields or options class to mark them as a suffix-option;
     * i.e., a config option where the key is only a suffix, with the prefix being dynamically
     * provided at runtime.
     */
    @Target({ElementType.FIELD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface SuffixOption {
        String value();
    }

    private Documentation() {}
}
