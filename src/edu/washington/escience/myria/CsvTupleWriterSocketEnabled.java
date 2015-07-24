package edu.washington.escience.myria;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import edu.washington.escience.myria.storage.ReadableTable;

/**
 * CsvTupleWriterSocketEnabled is a {@link TupleWriterSocketEnabled} that serializes tuples to a delimited file, usually a CSV. It uses a
 * {@link CSVPrinter} to do the underlying serialization. The fields to be output may contain special characters such as
 * newlines, because fields may be quoted (using double quotes '"'). Double quotation marks inside of fields are escaped
 * using the CSV-standard trick of replacing '"' with '""'.
 * 
 * CSV files should be compatible with Microsoft Excel.
 * 
 */
public class CsvTupleWriterSocketEnabled implements TupleWriterSocketEnabled, java.io.Serializable {

  /** The CSVWriter used to write the output. */
  private CSVPrinter csvPrinter;
    private OutputStream out;

  /**
   * Constructs a {@link CsvTupleWriterSocketEnabled} object that will produce an Excel-compatible comma-separated value (CSV) file
   * from the provided tuples.
   * 
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  public CsvTupleWriterSocketEnabled(OutputStream out) throws IOException {
    this(out, CSVFormat.DEFAULT);
  }

  /**
   * Constructs a {@link CsvTupleWriterSocketEnabled} object that will produce Excel-compatible comma-separated and tab-separated
   * files from the tuples in the provided queue.
   * 
   * @param separator the character used to separate fields in a line.
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  public CsvTupleWriterSocketEnabled(final char separator, OutputStream out) throws IOException {
    this(out, CSVFormat.DEFAULT.withDelimiter(separator));
  }

  /**
   * @param out the {@link OutputStream} to which the data will be written.
   * @param csvFormat the CSV format.
   * @throws IOException if there is an IO exception
   */
  private CsvTupleWriterSocketEnabled(OutputStream out, final CSVFormat csvFormat) throws IOException {
     out = edu.washington.escience.myria.operator.DataOutputSocketEnabled.getStream(this, java.io.OutputStream.class);
     //csvPrinter = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out)), csvFormat);
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {
      if(csvPrinter == null) {
out = edu.washington.escience.myria.operator.DataOutputSocketEnabled.getStream(this, java.io.OutputStream.class);
csvPrinter = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out)), CSVFormat.DEFAULT);
      }

    csvPrinter.printRecord(columnNames);
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {
      if(csvPrinter == null) {
out = edu.washington.escience.myria.operator.DataOutputSocketEnabled.getStream(this, java.io.OutputStream.class);
csvPrinter = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out)), CSVFormat.DEFAULT);
      }

    final String[] row = new String[tuples.numColumns()];
    /* Serialize every row into the output stream. */
    for (int i = 0; i < tuples.numTuples(); ++i) {
      for (int j = 0; j < tuples.numColumns(); ++j) {
        row[j] = tuples.getObject(j, i).toString();
      }
      csvPrinter.printRecord((Object[]) row);
    }
  }

  @Override
  public void done() throws IOException {
      if(csvPrinter == null) {
out = edu.washington.escience.myria.operator.DataOutputSocketEnabled.getStream(this, java.io.OutputStream.class);
csvPrinter = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out)), CSVFormat.DEFAULT);
      }    
    csvPrinter.flush();
    csvPrinter.close();
  }

  @Override
  public void error() throws IOException {
      if(csvPrinter == null) {
out = edu.washington.escience.myria.operator.DataOutputSocketEnabled.getStream(this, java.io.OutputStream.class);
csvPrinter = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out)), CSVFormat.DEFAULT);
      }
    try {
      csvPrinter.print("There was an error. Investigate the query status to see the message");
    } finally {
      csvPrinter.close();
    }
  }
}
