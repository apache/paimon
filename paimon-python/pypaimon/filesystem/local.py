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

import threading
import pyarrow
from pyarrow._fs import LocalFileSystem


class PaimonLocalFileSystem(LocalFileSystem):
    
    rename_lock = threading.Lock()

    def move(self, src, dst):
        with PaimonLocalFileSystem.rename_lock:
            file_info = self.get_file_info([dst])[0]
            result = file_info.type != pyarrow.fs.FileType.NotFound
            if (result is True):
                raise Exception("Target file already exists")

            super(PaimonLocalFileSystem, self).move(src, dst)