package org.smartdata.server.engine.data.net;

import org.apache.hadoop.net.SocketInputStream;
import org.apache.hadoop.net.SocketOutputStream;
import org.smartdata.server.engine.data.net.unix.DomainSocket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.channels.ReadableByteChannel;

/**
 * Represents a peer that we communicate with by using non-blocking I/O
 * on a Socket.
 */
public class NioInetPeer implements Peer {
  private final Socket socket;

  /**
   * An InputStream which simulates blocking I/O with timeouts using NIO.
   */
  private final SocketInputStream in;

  /**
   * An OutputStream which simulates blocking I/O with timeouts using NIO.
   */
  private final SocketOutputStream out;

  private final boolean isLocal;

  public NioInetPeer(Socket socket) throws IOException {
    this.socket = socket;
    this.in = new SocketInputStream(socket.getChannel(), 0);
    this.out = new SocketOutputStream(socket.getChannel(), 0);
    this.isLocal = socket.getInetAddress().equals(socket.getLocalAddress());
  }

  @Override
  public ReadableByteChannel getInputStreamChannel() {
    return in;
  }

  @Override
  public void setReadTimeout(int timeoutMs) throws IOException {
    in.setTimeout(timeoutMs);
  }

  @Override
  public int getReceiveBufferSize() throws IOException {
    return socket.getReceiveBufferSize();
  }

  @Override
  public boolean getTcpNoDelay() throws IOException {
    return socket.getTcpNoDelay();
  }

  @Override
  public void setWriteTimeout(int timeoutMs) throws IOException {
    out.setTimeout(timeoutMs);
  }

  @Override
  public boolean isClosed() {
    return socket.isClosed();
  }

  @Override
  public void close() throws IOException {
    // We always close the outermost streams-- in this case, 'in' and 'out'
    // Closing either one of these will also close the Socket.
    try {
      in.close();
    } finally {
      out.close();
    }
  }

  @Override
  public String getRemoteAddressString() {
    return socket.getRemoteSocketAddress().toString();
  }

  @Override
  public String getLocalAddressString() {
    return socket.getLocalSocketAddress().toString();
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return in;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return out;
  }

  @Override
  public boolean isLocal() {
    return isLocal;
  }

  @Override
  public String toString() {
    return "NioInetPeer(" + socket.toString() + ")";
  }

  @Override
  public DomainSocket getDomainSocket() {
    return null;
  }

  @Override
  public boolean hasSecureChannel() {
    return false;
  }
}
