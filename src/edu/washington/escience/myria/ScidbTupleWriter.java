package edu.washington.escience.myria;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.ArrayList;
import java.lang.StringBuilder;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.csv.CSVPrinter;
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
  private transient CSVPrinter csvPrinter;
  private final int numDims;
  static final long serialVersionUID = 1L;

  /**
   * Constructs a {@link CsvTupleWriter} object that will produce an Excel-compatible comma-separated value (CSV) file
   * from the provided tuples.
   * 
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  public ScidbTupleWriter(final OutputStream out, final int dims) throws IOException {   
    this(dims, out, CSVFormat.DEFAULT.withEscape(' ').withQuoteMode(QuoteMode.NONE));
  }

  /**
   * Constructs a {@link CsvTupleWriter} object that will produce Excel-compatible comma-separated and tab-separated
   * files from the tuples in the provided queue.
   * 
   * @param separator the character used to separate fields in a line.
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  public ScidbTupleWriter(final OutputStream out, final int dims, final char separator) throws IOException {
    this(dims, out, CSVFormat.DEFAULT.withDelimiter(separator).withEscape('\\').withQuoteMode(QuoteMode.NONE));
  }

  /**
   * @param out the {@link OutputStream} to which the data will be written.
   * @param csvFormat the CSV format.
   * @throws IOException if there is an IO exception
   */
  private ScidbTupleWriter(final int dims, final OutputStream out, final CSVFormat csvFormat) throws IOException {
    csvPrinter = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out)), csvFormat);
    this.numDims = dims;
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {  
    List<String> toReturn = new ArrayList<String>();
    StringBuilder toAppend = new StringBuilder("{");
    if(numDims>0){
      for(String column: columnNames.subList(0, numDims)){
        toAppend.append(column + ",");
      }
      toAppend.deleteCharAt(toAppend.lastIndexOf(","));    
    }
    else{
      toAppend.append("i");
    }
    toAppend.append("} ");
    toReturn.add(toAppend.toString() + columnNames.get(numDims));
    for(String column: columnNames.subList(numDims+1, columnNames.size())){
      toReturn.add(column);
    }
    csvPrinter.printRecord(toReturn);
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
    /* Serialize every row into the output stream. */
    for (int i = 0; i < tuples.numTuples(); ++i) {
      StringBuilder toAppend = new StringBuilder("{");
      // If the first columns are dimensions, pull them off
      if(numDims>0){
        for (int j = 0; j < numDims; ++j) {
          toAppend.append(tuples.getObject(j, i).toString() + ",");
        }
        toAppend.deleteCharAt(toAppend.lastIndexOf(","));
      }
      else{
        toAppend.append(Integer.toString(i));
      }
      toAppend.append("} ");
      row[0] = toAppend.toString() + formatString(types.get(numDims), tuples.getObject(numDims, i).toString());
      for (int j=numDims+1; j<tuples.numColumns(); j++){
        row[j-numDims] = formatString(types.get(j), tuples.getObject(j, i).toString());
      }
      csvPrinter.printRecord((Object[]) row);
    }
  }

  @Override
  public void done() throws IOException {
    csvPrinter.flush();
    csvPrinter.close();
  }

  @Override
  public void error() throws IOException {
    try {
      csvPrinter.print("There was an error. Investigate the query status to see the message");
    } finally {
      csvPrinter.close();
    }
  }
}
