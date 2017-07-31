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

#include "org_apache_hadoop.h"
#include "org_smartdata_erasurecode_NativeCodeLoader.h"

#ifdef UNIX
#include <dlfcn.h>
#include "config.h"
#endif // UNIX

#ifdef WINDOWS
#include "winutils.h"
#endif

#include <jni.h>



JNIEXPORT jboolean JNICALL Java_org_smartdata_erasurecode_NativeCodeLoader_buildSupportsOpenssl
  (JNIEnv *env, jclass clazz)
{
#ifdef SMART_OPENSSL_LIBRARY
  return JNI_TRUE;
#else
  return JNI_FALSE;
#endif
}

JNIEXPORT jboolean JNICALL Java_org_smartdata_erasurecode_NativeCodeLoader_buildSupportsIsal
  (JNIEnv *env, jclass clazz)
{
#ifdef SMART_ISAL_LIBRARY
  return JNI_TRUE;
#else
  return JNI_FALSE;
#endif
}

JNIEXPORT jstring JNICALL Java_org_smartdata_erasurecode_NativeCodeLoader_getLibraryName
  (JNIEnv *env, jclass clazz)
{
#ifdef UNIX
  Dl_info dl_info;
  int ret = dladdr(
      Java_org_smartdata_erasurecode_NativeCodeLoader_getLibraryName,
      &dl_info);
  return (*env)->NewStringUTF(env, ret==0 ? "Unavailable" : dl_info.dli_fname);
#endif

#ifdef WINDOWS
  LPWSTR filename = NULL;
  GetLibraryName(Java_org_smartdata_erasurecode_NativeCodeLoader_getLibraryName,
    &filename);
  if (filename != NULL)
  {
    return (*env)->NewString(env, filename, (jsize) wcslen(filename));
  }
  else
  {
    return (*env)->NewStringUTF(env, "Unavailable");
  }
#endif
}
