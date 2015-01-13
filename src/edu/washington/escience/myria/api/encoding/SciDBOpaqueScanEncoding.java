package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.SciDBOpaqueScan;

public class SciDBOpaqueScanEncoding extends LeafOperatorEncoding<SciDBOpaqueScan> {
  @Required
  public Schema schema;
  @Required
  public DataSource source;
  public Boolean isLittleEndian;

  @Override
  public SciDBOpaqueScan construct(ConstructArgs args) {
    if (isLittleEndian == null) {
      return new SciDBOpaqueScan(schema, source);
    } else {
      return new SciDBOpaqueScan(schema, source, isLittleEndian);
    }
  }

}