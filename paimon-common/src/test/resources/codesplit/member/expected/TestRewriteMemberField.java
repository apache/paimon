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

public class TestRewriteMemberField {
String[] rewrite$0 = new String[3];
int[] rewrite$1 = new int[3];
long[] rewrite$2 = new long[2];

{
rewrite$1[1] = 1;
rewrite$0[1] = "GGGGG";
}

    
    
    
    
    
    
    
    

    public TestRewriteMemberField(String s) {
        this.rewrite$0[0] = s;
    }

    public String myFun(String h) {
        int aa = rewrite$1[0];
        return rewrite$0[0] + aa + this.rewrite$0[2] + rewrite$1[0] + String.valueOf(rewrite$1[1]);
    }
}
