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
package org.smartdata.server.engine.data.net;

import org.smartdata.server.engine.data.net.unix.DomainSocket;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ReadableByteChannel;

public interface Peer extends Closeable {
  /**
   * @return                The input stream channel associated with this
   *                        peer, or null if it has none.
   */
  ReadableByteChannel getInputStreamChannel();

  /**
   * Set the read timeout on this peer.
   *
   * @param timeoutMs       The timeout in milliseconds.
   */
  void setReadTimeout(int timeoutMs) throws IOException;

  /**
   * @return                The receive buffer size.
   */
  int getReceiveBufferSize() throws IOException;

  /**
   * @return                True if TCP_NODELAY is turned on.
   */
  boolean getTcpNoDelay() throws IOException;

  /**
   * Set the write timeout on this peer.
   *
   * Note: this is not honored for BasicInetPeer.
   *
   * @param timeoutMs       The timeout in milliseconds.
   */
  void setWriteTimeout(int timeoutMs) throws IOException;

  /**
   * @return                true only if the peer is closed.
   */
  boolean isClosed();

  /**
   * Close the peer.
   *
   * It's safe to re-close a Peer that is already closed.
   */
  void close() throws IOException;

  /**
   * @return               A string representing the remote end of our
   *                       connection to the peer.
   */
  String getRemoteAddressString();

  /**
   * @return               A string representing the local end of our
   *                       connection to the peer.
   */
  String getLocalAddressString();

  /**
   * @return               An InputStream associated with the Peer.
   *                       This InputStream will be valid until you close
   *                       this peer with Peer#close.
   */
  InputStream getInputStream() throws IOException;

  /**
   * @return               An OutputStream associated with the Peer.
   *                       This OutputStream will be valid until you close
   *                       this peer with Peer#close.
   */
  OutputStream getOutputStream() throws IOException;

  /**
   * @return               True if the peer resides on the same
   *                       computer as we.
   */
  boolean isLocal();

  /**
   * @return               The DomainSocket associated with the current
   *                       peer, or null if there is none.
   */
  DomainSocket getDomainSocket();

  /**
   * Return true if the channel is secure.
   *
   * @return               True if our channel to this peer is not
   *                       susceptible to man-in-the-middle attacks.
   */
  boolean hasSecureChannel();
}