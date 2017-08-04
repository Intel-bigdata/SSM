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

package org.smartdata.erasurecode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A helper to load the native smart code i.e. libsmart.so.
 * This handles the fallback to either the bundled libsmart-Linux-i386-32.so
 * or the default java implementations where appropriate.
 *  
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class NativeCodeLoader {

  private static final Log LOG =
    LogFactory.getLog(org.smartdata.erasurecode.NativeCodeLoader.class);
  
  private static boolean nativeCodeLoaded = false;
  
  static {
    // Try to load native smart library and set fallback flag appropriately
    if(LOG.isDebugEnabled()) {
      LOG.debug("Trying to load the custom-built native-smart library...");
    }
    try {
      System.loadLibrary("smart");
      LOG.debug("Loaded the native-smart library");
      nativeCodeLoaded = true;
    } catch (Throwable t) {
      // Ignore failure to load
      if(LOG.isDebugEnabled()) {
        LOG.debug("Failed to load native-smart with error: " + t);
        LOG.debug("java.library.path=" +
            System.getProperty("java.library.path"));
      }
    }
    
    if (!nativeCodeLoaded) {
      LOG.warn("Unable to load native-smart library for your platform... " +
               "using builtin-java classes where applicable");
    }
  }

  private NativeCodeLoader() {}

  /**
   * Check if native-smart code is loaded for this platform.
   * 
   * @return <code>true</code> if native-smart is loaded,
   *         else <code>false</code>
   */
  public static boolean isNativeCodeLoaded() {
    return nativeCodeLoaded;
  }

  /**
   * Returns true only if this build was compiled with support for snappy.
   */
  public static native boolean buildSupportsSnappy();

  /**
   * Returns true only if this build was compiled with support for ISA-L.
   */
  public static native boolean buildSupportsIsal();

  /**
  * Returns true only if this build was compiled with support for ZStandard.
   */
  public static native boolean buildSupportsZstd();

  /**
   * Returns true only if this build was compiled with support for openssl.
   */
  public static native boolean buildSupportsOpenssl();

  public static native String getLibraryName();

}
