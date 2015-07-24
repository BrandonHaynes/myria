package edu.washington.escience.myria.operator;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.TupleWriterSocketEnabled;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * DataOutputSocketEnabled is a {@link RootOperator} that can be used to serialize data in a streaming fashion. It consumes
 * {@link TupleBatch}es from its child and passes them to a {@link TupleWriterSocketEnabled}.
 * 
 * 
 */
public final class DataOutputSocketEnabled extends RootOperator {
    private static class LazyStream extends java.io.OutputStream {
	private java.net.ServerSocket serverSocket;
	private java.net.Socket socket;
	private java.io.OutputStream stream;

	private java.io.OutputStream getStream() throws IOException {
	    if(serverSocket == null) {
		serverSocket = new java.net.ServerSocket(9998);
		socket = serverSocket.accept();
		stream = socket.getOutputStream();
	    }

	    return stream;
	}

    @Override
	public void close()
           throws IOException
        { getStream().close(); }

    @Override
	public void flush() throws IOException { getStream().flush(); }

    @Override
	public void write(byte[] bytes) throws IOException { getStream().write(bytes); }

    @Override
	public void write(byte[] bytes, int offset, int length) throws IOException { getStream().write(bytes, offset, length); }

    @Override
	public void write(int b) throws IOException { getStream().write(b); }
    }


  private static java.net.ServerSocket _inputSocket;
  private static java.net.ServerSocket _outputSocket;
  public synchronized static <T> T getStream(Object instance, Class<T> ioClass) {
      //      try {
	  if(ioClass.equals(java.io.OutputStream.class)) {
	      //if(_outputSocket == null)
 //	  _inputSocket = new java.net.Socket("localhost", 9998)
	      //_outputSocket = new java.net.ServerSocket(9998);
	      //return (T)_outputSocket.accept().getOutputStream();
	      return (T)new LazyStream();
	  }
	  else {
	      if(_outputSocket == null)
		  //	  _outputSocket = new java.net.ServerSocket(9998);
 {
     /*
     //_outputSocket = new java.net.Socket("localhost", 9998);
     int attempts = 60;

	      while(attempts-- > 0) {
		  try {
		      _outputSocket = new java.net.Socket("localhost", 9998);
		      break;
		  } catch(java.net.ConnectException e) {
		      if(attempts > 0)
			  try {
			      Thread.sleep(1000);
			  } catch(java.lang.InterruptedException inte) {
			      throw new java.io.IOException(inte);
			  }
		      else
			  throw e;
		  }
	      }
     */
 }

	      //	      return (T)_outputSocket.accept().getOutputStream(
return (T)new LazyStream();

	  }
	  //      } catch(IOException e) {
	  //throw new RuntimeException(e);
	  // }
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The class that will serialize the tuple batches. */
  private final TupleWriterSocketEnabled tupleWriter;
  /** Whether this object has finished. */
  private boolean done = false;

  /**
   * Instantiate a new DataOutputSocketEnabled operator, which will stream its tuples to the specified {@link TupleWriterSocketEnabled}.
   * 
   * @param child the source of tuples to be streamed.
   * @param writer the {@link TupleWriterSocketEnabled} which will serialize the tuples.
   */
  public DataOutputSocketEnabled(final Operator child, final TupleWriterSocketEnabled writer) {
    super(child);
    tupleWriter = writer;
  }

  @Override
  protected void childEOI() throws DbException {
    /* Do nothing. */
  }

  @Override
  protected void childEOS() throws DbException {
    try {
      tupleWriter.done();
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
      tupleWriter.writeColumnHeaders(getChild().getSchema().getColumnNames());
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
