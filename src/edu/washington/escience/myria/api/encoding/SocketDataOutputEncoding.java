package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.io.IOException;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.MoreObjects;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SocketDataOutput;
import edu.washington.escience.myria.api.MyriaApiException;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class SocketDataOutputEncoding extends UnaryOperatorEncoding<SocketDataOutput> {
  @Required
  public int port;
  @Required
  public int numDims;
  @Required
  public int chunkSize;
  @Required 
  public boolean csvFormat;
  @Required
  public boolean isLittleEndian;

  @Override
  public SocketDataOutput construct(ConstructArgs args){
    /* default overwrite to {@code false}, so we append. */
    try{
      return new SocketDataOutput(null, port, numDims, csvFormat, chunkSize, isLittleEndian);
    }
    catch(IOException e){
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR,e);
    }
  }
}