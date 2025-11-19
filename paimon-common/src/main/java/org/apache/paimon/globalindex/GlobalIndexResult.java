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

package org.apache.paimon.globalindex;

import org.apache.paimon.utils.RoaringBitmap64;

/**
 * Global index result represents row ids.
 *
 * <p>TODO introduce ranges interface
 */
public interface GlobalIndexResult {

    RoaringBitmap64 result();

    static GlobalIndexResult createEmpty() {
        return RoaringBitmap64::new;
    }

    default GlobalIndexResult and(GlobalIndexResult globalIndexResult) {
        RoaringBitmap64 result0 = result();
        RoaringBitmap64 result1 = globalIndexResult.result();
        return () -> RoaringBitmap64.and(result0, result1);
    }

    default GlobalIndexResult or(GlobalIndexResult globalIndexResult) {
        RoaringBitmap64 result0 = result();
        RoaringBitmap64 result1 = globalIndexResult.result();
        return () -> RoaringBitmap64.or(result0, result1);
    }
}
