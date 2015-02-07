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
  private int numDims;
  private ServerSocket serverSocket;

  private boolean needsHeader;

  /**
   * Instantiate a new DataOutput operator, which will stream its tuples to the specified {@link TupleWriter}.
   * 
   * @param child the source of tuples to be streamed.
   * @param writer the {@link TupleWriter} which will serialize the tuples.
   */
  public SocketDataOutput(final Operator child, final TupleWriter writer) {
    super(child);
    tupleWriter = writer;
  }

// Have the tuplewriter write to a socket
  public SocketDataOutput(final Operator child, final int port, final boolean header, final int numDims) 
    throws IOException{
    super(child);
    this.port = port;
    this.numDims = numDims;
    needsHeader=header;
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
    if(needsHeader){
      try {
        serverSocket = new ServerSocket(port);
        tupleWriter = new ScidbTupleWriter(serverSocket.accept().getOutputStream(), numDims);
        tupleWriter.writeColumnHeaders(getChild().getSchema().getColumnNames());
     } catch (IOException e) {
        throw new DbException(e);
      }
    }
  }

  @Override
  protected void cleanup() throws IOException {
    if (!done) {
      tupleWriter.error();
    }
  }
}
