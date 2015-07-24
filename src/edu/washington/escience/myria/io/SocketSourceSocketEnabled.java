package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.Thread;

import java.net.Socket;
import java.net.ServerSocket;
import java.net.ConnectException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SocketSourceSocketEnabled implements DataSourceSocketEnabled, Serializable {
  private static final int DEFAULT_ATTEMPTS = 60;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @JsonProperty
  private final int port;
  @JsonProperty
  private final String hostname;
  @JsonProperty
  private int attempts;

  private ServerSocket serverSocket = null;
  private Socket socket = null;

  @JsonCreator
  public SocketSourceSocketEnabled(@JsonProperty(value = "port", required = true) final int port,
                      @JsonProperty(value = "hostname", required = false) final String hostname,
                      @JsonProperty(value = "attempts", required = false) final int attempts) {
    if(attempts < 0)
      throw new IllegalArgumentException("Number of attempts must be positive.");
    else if(port <= 0)
      throw new IllegalArgumentException("Socket port number must be positive.");

    this.hostname = hostname;
    this.port = port;
    this.attempts = attempts != 0 ? attempts : DEFAULT_ATTEMPTS;
  }

  @Override
  public InputStream getInputStream() throws IOException {
return edu.washington.escience.myria.operator.FileScanSocketEnabled.getStream(this, java.io.InputStream.class);
}

  public int getPort() {
    return port;
  }

  //@Override
  public void close() throws IOException {
    if(serverSocket != null)
      serverSocket.close();
    if(socket != null)
      socket.close();
    socket = null;
    serverSocket = null;
  }
}
