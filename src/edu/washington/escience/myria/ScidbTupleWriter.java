package edu.washington.escience.myria;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.ArrayList;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.math.NumberUtils;

import edu.washington.escience.myria.storage.ReadableTable;

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
  private int numDims;
  static final long serialVersionUID = 1L;

  /**
   * Constructs a {@link CsvTupleWriter} object that will produce an Excel-compatible comma-separated value (CSV) file
   * from the provided tuples.
   * 
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  public ScidbTupleWriter(final OutputStream out, final int dims) throws IOException {   
    this(out, CSVFormat.DEFAULT.withEscape('\\').withQuoteMode(QuoteMode.NONE));
    numDims = dims;
  }

  /**
   * Constructs a {@link CsvTupleWriter} object that will produce Excel-compatible comma-separated and tab-separated
   * files from the tuples in the provided queue.
   * 
   * @param separator the character used to separate fields in a line.
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  public ScidbTupleWriter(final char separator, final OutputStream out) throws IOException {
    this(out, CSVFormat.DEFAULT.withDelimiter(separator).withEscape('\\').withQuoteMode(QuoteMode.NONE));
  }

  /**
   * @param out the {@link OutputStream} to which the data will be written.
   * @param csvFormat the CSV format.
   * @throws IOException if there is an IO exception
   */
  private ScidbTupleWriter(final OutputStream out, final CSVFormat csvFormat) throws IOException {
    csvPrinter = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out)), csvFormat);
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {  
    List<String> toReturn = new ArrayList<String>();
    String toAppend = "{";
    if(numDims>0){
      for(int i=0; i<numDims; i++){
        toAppend += columnNames.get(i) + ",";
      }
      toAppend = toAppend.substring(0, toAppend.length()-1);      
    }
    else{
      toAppend += "i";
    }
    toAppend += "} ";
    toReturn.add(toAppend + columnNames.get(numDims));
    for(int i=numDims+1; i<columnNames.size(); i++){
      toReturn.add(columnNames.get(i));
    }
    csvPrinter.printRecord(toReturn);
  }

  public String formatString(String value){
    if(NumberUtils.isNumber(value)){
      return value;
    }
    else{
      return "\"" + value + "\"";
   }
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {
    final String[] row = new String[tuples.numColumns()-numDims];
    /* Serialize every row into the output stream. */
    for (int i = 0; i < tuples.numTuples(); ++i) {
      String toAppend = "{";
      // If the first columns are dimensions, pull them off
      if(numDims>0){
        for (int j = 0; j < numDims; ++j) {
          toAppend +=  tuples.getObject(j, i).toString() + ",";
        }
        toAppend = toAppend.substring(0, toAppend.length()-1);
      }
      else{
        toAppend += Integer.toString(i);
      }
      row[0] = toAppend + "} " + formatString(tuples.getObject(numDims, i).toString());
      for (int j=numDims+1; j<tuples.numColumns(); j++){
        row[j-numDims] = formatString(tuples.getObject(j, i).toString());
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
