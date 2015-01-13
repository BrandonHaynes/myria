package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.ImplicitDimensionBinaryFileScan;

public class ImplicitDimensionBinaryFileScanEncoding extends LeafOperatorEncoding<ImplicitDimensionBinaryFileScan> {
  @Required
  public Schema schema;
  @Required
  public DataSource source;
  @Required
  public List<Integer> dimensions;
  public Boolean isLittleEndian;

  @Override
  public ImplicitDimensionBinaryFileScan construct(ConstructArgs args) {
    if (isLittleEndian == null) {
      return new ImplicitDimensionBinaryFileScan(schema, source, dimensions);
    } else {
      return new ImplicitDimensionBinaryFileScan(schema, source, dimensions, isLittleEndian);
    }
  }

}