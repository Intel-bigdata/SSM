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
package org.smartdata.erasurecode.codec;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.smartdata.erasurecode.ErasureCodecOptions;
import org.smartdata.erasurecode.coder.ErasureDecoder;
import org.smartdata.erasurecode.coder.ErasureEncoder;
import org.smartdata.erasurecode.coder.HHXORErasureDecoder;
import org.smartdata.erasurecode.coder.HHXORErasureEncoder;

/**
 * A Hitchhiker-XOR erasure codec.
 */
@InterfaceAudience.Private
public class HHXORErasureCodec extends ErasureCodec {

  public HHXORErasureCodec(Configuration conf, ErasureCodecOptions options) {
    super(conf, options);
  }

  @Override
  public ErasureEncoder createEncoder() {
    return new HHXORErasureEncoder(getCoderOptions());
  }

  @Override
  public ErasureDecoder createDecoder() {
    return new HHXORErasureDecoder(getCoderOptions());
  }
}
