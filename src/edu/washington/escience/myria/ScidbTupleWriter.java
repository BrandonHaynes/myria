package edu.washington.escience.myria;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.ArrayList;

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
public class ScidbTupleWriter implements TupleWriter {

  /** The CSVWriter used to write the output. */
  private transient PrintWriter printer;
  private final int numDims;
  private final int chunkSize;
  static final long serialVersionUID = 1L;

  /**
   * Constructs a {@link CsvTupleWriter} object that will produce an Excel-compatible comma-separated value (CSV) file
   * from the provided tuples.
   * 
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  public ScidbTupleWriter(final OutputStream out, final int dims, final int chunkSize) throws IOException {   
    this.printer = new PrintWriter(out);
    this.numDims = dims;
    this.chunkSize = chunkSize;
  }

    /**
   * Constructs a {@link CsvTupleWriter} object that will produce an Excel-compatible comma-separated value (CSV) file
   * from the provided tuples.  No specified chunk size.
   * 
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  public ScidbTupleWriter(final OutputStream out, final int dims) throws IOException {   
    this.printer = new PrintWriter(out);
    this.numDims = dims;
    this.chunkSize = 1<<24;
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {  
    // SciDB doesn't take column headers.
  }

  public String formatString(Type type, String value){
    if(type.equals(Type.INT_TYPE) 
        || type.equals(Type.FLOAT_TYPE) 
        || type.equals(Type.DOUBLE_TYPE) 
        || type.equals(Type.LONG_TYPE)){
      return value;
    }
    else{
      return "\"" + value + "\"";
   }
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {
    final String[] row = new String[tuples.numColumns()-numDims];
    List<Type> types = tuples.getSchema().getColumnTypes();
    int numTuples = tuples.numTuples();
    int index = 0;
    printer.print("{0}[\n");
    /* Serialize every row into the output stream. */
    for (int i = 0; i < numTuples; ++i) {
      // If the first columns are dimensions, pull them off     
      printer.print('(');
      printer.print(formatString(types.get(numDims), tuples.getObject(numDims, i).toString()));      
      for (int j=numDims+1; j<tuples.numColumns(); j++){
        printer.print(',');
        printer.print(formatString(types.get(j), tuples.getObject(j, i).toString()));
      }
      printer.print(')');

      //check whether the next tuple fits in the current chunk
      if((i+1)==numTuples){
        printer.print("\n]");
      }
      else if((i+1)%chunkSize==0){
        index+=chunkSize;
        printer.print("\n]\n{");
        printer.print(Integer.toString(index));
        printer.print("}[\n");
      } 
      else{
        printer.print(',');
      }
    }
  }

  @Override
  public void done() throws IOException {
    printer.close();
  }

  @Override
  public void error() throws IOException {
    try {
      printer.print("There was an error. Investigate the query status to see the message");
    } finally {
      printer.close();
    }
  }
}
