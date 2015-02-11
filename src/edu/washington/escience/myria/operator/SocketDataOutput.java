package edu.washington.escience.myria.operator;

import java.io.IOException;
import java.io.OutputStream;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.ScidbTupleWriter;
import java.net.*;

/**
 * DataOutput is a {@link RootOperator} that can be used to serialize data in a streaming fashion. It consumes
 * {@link TupleBatch}es from its child and passes them to a {@link TupleWriter}.
 * 
 * 
 */
public final class SocketDataOutput extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The class that will serialize the tuple batches. */
  private transient TupleWriter tupleWriter;
  /** Whether this object has finished. */
  private boolean done = false;
  private int port;
  private final int numDims;
  private ServerSocket serverSocket;
  private final int chunkSize;

// Have the tuplewriter write to a socket
  public SocketDataOutput(final Operator child, final int port, final int numDims) 
    throws IOException{
    super(child);
    this.port = port;
    this.numDims = numDims;
    this.chunkSize=0;
  }

// Have the tuplewriter write to a socket
  public SocketDataOutput(final Operator child, final int port, final int numDims, final int chunkSize) 
    throws IOException{
    super(child);
    this.port = port;
    this.numDims = numDims;
    this.chunkSize = chunkSize;
  }

  @Override
  protected void childEOI() throws DbException {
    /* Do nothing. */
  }

  @Override
  protected void childEOS() throws DbException {
    try {
      tupleWriter.done();
      serverSocket.close();
    } catch (IOException e) {
      throw new DbException(e);
    }
    done = true;
  }

  @Override
  protected void consumeTuples(final TupleBatch tuples) throws DbException {
    try {
      tupleWriter.writeTuples(tuples);
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    try {
      serverSocket = new ServerSocket(port);
      if(this.chunkSize==0){
       tupleWriter = new ScidbTupleWriter(serverSocket.accept().getOutputStream(), numDims);
      }
      else{
        tupleWriter = new ScidbTupleWriter(serverSocket.accept().getOutputStream(), numDims, chunkSize);
      }
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @Override
  protected void cleanup() throws IOException {
    if (!done) {
      tupleWriter.error();
    }
  }
}
