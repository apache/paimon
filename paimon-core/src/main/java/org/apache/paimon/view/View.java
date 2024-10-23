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

package org.apache.paimon.view;

import org.apache.paimon.types.RowType;

import java.util.Map;
import java.util.Optional;

/** Interface for view definition. */
public interface View {

    /** A name to identify this view. */
    String name();

    /** Full name (including database) to identify this view. */
    String fullName();

    /** Returns the row type of this view. */
    RowType rowType();

    /** Returns the view representation. */
    String query();

    /** Optional comment of this view. */
    Optional<String> comment();

    /** Options of this view. */
    Map<String, String> options();

    /** Copy this view with adding dynamic options. */
    View copy(Map<String, String> dynamicOptions);
}
