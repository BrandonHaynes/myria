package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSourceSocketEnabled;
import edu.washington.escience.myria.operator.FileScanSocketEnabled;
import edu.washington.escience.myria.operator.FileScan;

public class FileScanSocketEnabledEncoding extends LeafOperatorEncoding<FileScanSocketEnabled> {
  @Required
  public Schema schema;
  @Required
  public DataSourceSocketEnabled source;
  public Character delimiter;
  public Character quote;
  public Character escape;
  public Integer skip;

  @Override
  public FileScanSocketEnabled construct(ConstructArgs args) {
      return new FileScanSocketEnabled(source, schema, delimiter, quote, escape, skip);
  }
}