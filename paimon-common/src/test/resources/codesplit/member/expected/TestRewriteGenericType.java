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

public class TestRewriteGenericType {
java.util.List<Integer>[] rewrite$0 = new java.util.List[1];
java.util.List<String>[] rewrite$1 = new java.util.List[2];

{
rewrite$1[0] = new java.util.ArrayList<>();
rewrite$0[0] = new java.util.ArrayList<>();
rewrite$1[1] = new java.util.ArrayList<>();
}

    
    
    

    public String myFun() {
        String aa = rewrite$1[0].get(0);
        long bb = rewrite$0[0].get(1);
        String cc = rewrite$1[1].get(2);
        return cc + bb + aa;
    }
}
