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

public class TestNotSplitJavaCode {
    private int a = 1;
    public final int[] b;

    public TestNotSplitJavaCode(int[] b) {
        this.b = b;
    }

    public void myFun(int a) {
        this.a = a;
        b[0] += b[1];
        b[1] += b[2];
        b[2] += b[3];
        int b1 = b[0] + b[1];
        int b2 = b[1] + b[2];
        int b3 = b[2] + b[0];
        for (int x : b) {
            System.out.println(x + b1 + b2 + b3);
        }

        if (b1 > 0) {
            b[0] += 100;
            if (b2 > 0) {
                b[1] += 100;
                if (b3 > 0) {
                    b[2] += 100;
                } else {
                    b[2] += 50;
                }
            } else {
                b[1] += 50;
            }
        } else {
            b[0] += 50;
        }
    }
}
