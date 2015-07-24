package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.TupleWriterSocketEnabled;
import edu.washington.escience.myria.operator.DataOutputSocketEnabled;
import edu.washington.escience.myria.CsvTupleWriterSocketEnabled;

public class DataOutputSocketEnabledEncoding extends UnaryOperatorEncoding<DataOutputSocketEnabled> {
    //@Required
    //public TupleWriterSocketEnabled tupleWriter;
  public int numDims;
  public boolean csvFormat;
  public boolean isLittleEndian;
  public int port;

  @Override
  public DataOutputSocketEnabled construct(ConstructArgs args) {
      try {
      TupleWriterSocketEnabled tupleWriter = new CsvTupleWriterSocketEnabled(DataOutputSocketEnabled.getStream(this, java.io.OutputStream.class));
      return new DataOutputSocketEnabled(null, tupleWriter);
      } catch(java.io.IOException e) {
	  throw new java.lang.RuntimeException(e);
      }
  }
}