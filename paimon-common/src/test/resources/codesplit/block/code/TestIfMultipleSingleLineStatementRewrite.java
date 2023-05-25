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

public class TestIfMultipleSingleLineStatementRewrite {
    public void myFun1(int[] a, int[] b) throws RuntimeException {
        if (a[0] == 0) {
            a[11] = b[0];
            a[12] = b[0];
            if (a[2] == 0) {
                a[21] = 1;
                a[22] = 1;
            } else {
                a[23] = b[2];
                a[24] = b[2];
            }

            a[13] = b[0];
            a[14] = b[0];
        } else {
            a[0] = b[0];
            a[1] = b[1];
            a[2] = b[2];
        }
    }
}
