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

public class SocketSource implements DataSource, Serializable {
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
  public SocketSource(@JsonProperty(value = "port", required = true) final int port,
                      @JsonProperty(value = "hostname", required = false) final String hostname,
                      @JsonProperty(value = "attempts", required = false) final int attempts) {
    this.hostname = hostname;
    this.port = port;
    this.attempts = attempts != 0 ? attempts : DEFAULT_ATTEMPTS;

    if(attempts <= 0)
      throw new IllegalArgumentException("Number of attempts must be positive.");
    else if(port <= 0)
      throw new IllegalArgumentException("Socket port number must be positive.");
  }

  @Override
  public InputStream getInputStream() throws IOException {
    if(hostname == null) {
      serverSocket = new ServerSocket(port);
      return serverSocket.accept().getInputStream();
    } else {
      while(attempts-- > 0) {
        try {
          socket = new Socket(hostname, port);          
          break;
        } catch(ConnectException e) { 
          if(attempts > 0)
            try {
              Thread.sleep(1000);
            } catch(InterruptedException inte) {
              throw new IOException(inte);
            }
          else
            throw e;
        }
      }

      return socket.getInputStream();    
    }
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
