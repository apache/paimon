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

public class TestIfInsideWhileLoopRewrite {

    int counter = 0;

    public void myFun(int[] a, int[] b, int[] c ) throws RuntimeException {

        a[0] += b[1];
        b[1] += a[1];
        while (counter < 10) {
            c[counter] = a[0] + 1000;
            System.out.println(c);
            if (a[counter] > 0) {
                b[counter] = a[counter] * 2;
                c[counter] = b[counter] * 2;
                System.out.println(b[counter]);
            } else {
                b[counter] = a[counter] * 3;
                System.out.println(b[counter]);
            }

            a[2] += b[2];
            b[3] += a[3];
            if (a[0] > 0) {
                System.out.println("Hello");
            } else {
                System.out.println("World");
            }

            counter--;
        }

        a[4] += b[4];
        b[5] += a[5];

        if (a.length < 100) {
            while (counter < 10) {
                c[counter] = a[0] + 1000;
                System.out.println(c);
                if (a[counter] > 0) {
                    b[counter] = a[counter] * 2;
                    c[counter] = b[counter] * 2;
                    System.out.println(b[counter]);
                } else {
                    b[counter] = a[counter] * 3;
                    System.out.println(b[counter]);
                }

                a[2] += b[2];
                b[3] += a[3];
                if (a[0] > 0) {
                    System.out.println("Hello");
                } else {
                    System.out.println("World");
                }

                counter--;
                System.out.println("World ffff");
            }
        } else {
            while (counter < 10) {
                b[counter] = b[counter]++;
                counter++;
            }

            System.out.println("World Else");
            System.out.println("World Else 2");
        }
    }
}
