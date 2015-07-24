package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.ImplicitDimensionFileScan;

public class ImplicitDimensionFileScanEncoding extends LeafOperatorEncoding<ImplicitDimensionFileScan> {
  @Required
  public Schema schema;
  @Required
  public DataSource source;
  @Required
  public List<Integer> dimensions;
  public Character delimiter;
  public Character quote;
  public Character escape;
  public Integer skip;

  @Override
  public ImplicitDimensionFileScan construct(ConstructArgs args) {
      return new ImplicitDimensionFileScan(source, schema, delimiter, quote, escape, skip, dimensions);
  }
}