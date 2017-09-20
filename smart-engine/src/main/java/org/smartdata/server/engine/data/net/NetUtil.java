package org.smartdata.server.engine.data.net;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;

public class NetUtil {

  public static Peer peerFromSocket(Socket socket)
      throws IOException {
    Peer peer;
    boolean success = false;
    try {
      // TCP_NODELAY is crucial here because of bad interactions between
      // Nagle's Algorithm and Delayed ACKs. With connection keepalive
      // between the client and DN, the conversation looks like:
      //   1. Client -> DN: Read block X
      //   2. DN -> Client: data for block X
      //   3. Client -> DN: Status OK (successful read)
      //   4. Client -> DN: Read block Y
      // The fact that step #3 and #4 are both in the client->DN direction
      // triggers Nagling. If the DN is using delayed ACKs, this results
      // in a delay of 40ms or more.
      //
      // TCP_NODELAY disables nagling and thus avoids this performance
      // disaster.
      socket.setTcpNoDelay(true);
      SocketChannel channel = socket.getChannel();
      if (channel == null) {
        peer = new BasicInetPeer(socket);
      } else {
        peer = new NioInetPeer(socket);
      }
      success = true;
      return peer;
    } finally {
      if (!success) {
        // peer is always null so no need to call peer.close().
        socket.close();
      }
    }
  }

  public static void cleanup(Logger log, java.io.Closeable... closeables) {
    for (java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch(Throwable e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing " + c, e);
          }
        }
      }
    }
  }
}
