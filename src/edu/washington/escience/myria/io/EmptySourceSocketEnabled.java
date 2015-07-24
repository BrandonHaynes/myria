package edu.washington.escience.myria.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A data source with no tuples.
 * 
 * @author whitaker
 */
public class EmptySourceSocketEnabled implements DataSourceSocketEnabled {

  @Override
  public InputStream getInputStream() throws IOException {
return edu.washington.escience.myria.operator.FileScanSocketEnabled.getStream(this, java.io.InputStream.class);
}
}
