#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

class PVFSConfig:
    DEFAULT_CACHE_SIZE = 20
    CACHE_SIZE = "cache_size"
    CACHE_EXPIRED_TIME = "cache_expired_time"
    DEFAULT_CACHE_EXPIRED_TIME = 3600
    OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId"
    OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret"
    OSS_SECURITY_TOKEN = "fs.oss.securityToken"
    OSS_ENDPOINT = "fs.oss.endpoint"