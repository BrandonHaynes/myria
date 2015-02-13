package edu.washington.escience.myria;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.DataOutputStream;
import java.io.DataOutput;
import java.util.List;
import java.util.ArrayList;
import edu.washington.escience.myria.DbException;
import com.google.common.io.LittleEndianDataOutputStream;

import org.apache.commons.lang3.math.NumberUtils;

import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.Type;

/**
 * CsvTupleWriter is a {@link TupleWriter} that serializes tuples to a delimited file, usually a CSV. It uses a
 * {@link CSVPrinter} to do the underlying serialization. The fields to be output may contain special characters such as
 * newlines, because fields may be quoted (using double quotes '"'). Double quotation marks inside of fields are escaped
 * using the CSV-standard trick of replacing '"' with '""'.
 * 
 * CSV files should be compatible with Microsoft Excel.
 * 
 */
public class ScidbBinaryTupleWriter implements TupleWriter {

  /** The CSVWriter used to write the output. */
  private transient DataOutput printer;
  private final int numDims;
  private final boolean isLittleEndian;

  /**
   * Constructs a {@link CsvTupleWriter} object that will produce an Excel-compatible comma-separated value (CSV) file
   * from the provided tuples.
   * 
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  public ScidbBinaryTupleWriter(final OutputStream out, final int dims, final boolean isLittleEndian) throws IOException {   
    this.numDims = dims;
    this.isLittleEndian = isLittleEndian;
    if(isLittleEndian){
      this.printer = new LittleEndianDataOutputStream(out);
    }
    else{
      this.printer = new DataOutputStream(out);
    }
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {  
    // SciDB doesn't take column headers.
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {
    final String[] row = new String[tuples.numColumns()-numDims];
    List<Type> types = tuples.getSchema().getColumnTypes();
    int numTuples = tuples.numTuples();
    /* Serialize every row into the output stream. */
    for (int i = 0; i < numTuples; ++i) {
      // If the first columns are dimensions, ignore them      
      for (int j=numDims; j<tuples.numColumns(); j++){
        Type type = types.get(j);
        if(type.equals(Type.INT_TYPE)){
          printer.writeInt(tuples.getInt(j, i));
        }
        else if(type.equals(Type.FLOAT_TYPE)){
          printer.writeFloat(tuples.getFloat(j, i));
        }
        else if(type.equals(Type.DOUBLE_TYPE)){
          printer.writeDouble(tuples.getDouble(j, i));
        }
        else if(type.equals(Type.LONG_TYPE)){
          printer.writeLong(tuples.getLong(j, i));
        }
        else if(type.equals(Type.BOOLEAN_TYPE)){
          printer.writeBoolean(tuples.getBoolean(j, i));
        }
        else{
          throw new IOException("Type not supported for binary export.");
        }
      }
    }
  }

  @Override
  public void done() throws IOException {
    if(printer instanceof DataOutputStream){
      ((DataOutputStream) printer).close();
    }
    else{
      ((LittleEndianDataOutputStream) printer).close();
    }
  }

  @Override
  public void error() throws IOException {
    try {
      printer.writeChars("There was an error. Investigate the query status to see the message");
    } finally {
      done();
    }
  }
}
