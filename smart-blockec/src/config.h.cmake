/**
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
#ifndef CONFIG_H
#define CONFIG_H

#cmakedefine SMART_ZLIB_LIBRARY "@SMART_ZLIB_LIBRARY@"
#cmakedefine SMART_BZIP2_LIBRARY "@SMART_BZIP2_LIBRARY@"
#cmakedefine SMART_SNAPPY_LIBRARY "@SMART_SNAPPY_LIBRARY@"
#cmakedefine SMART_ZSTD_LIBRARY "@SMART_ZSTD_LIBRARY@"
#cmakedefine SMART_OPENSSL_LIBRARY "@SMART_OPENSSL_LIBRARY@"
#cmakedefine SMART_ISAL_LIBRARY "@SMART_ISAL_LIBRARY@"
#cmakedefine HAVE_SYNC_FILE_RANGE
#cmakedefine HAVE_POSIX_FADVISE

#endif
