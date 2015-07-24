package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * A data source that simply wraps an input stream.
 */
public class InputStreamSourceSocketEnabled implements DataSourceSocketEnabled {

  /** The source input stream. */
  private InputStream stream;

  /**
   * Returns a {@link DataSourceSocketEnabled} that wraps the specified {@link InputStream}.
   * 
   * @param stream the source of data.
   */
  public InputStreamSourceSocketEnabled(InputStream stream) {
     stream = edu.washington.escience.myria.operator.FileScanSocketEnabled.getStream(this, java.io.InputStream.class);
this.stream = Objects.requireNonNull(stream, "Parameter stream to InputStreamSourceSocketEnabled may not be null");
  }

  @Override
  public InputStream getInputStream() throws IOException {
return edu.washington.escience.myria.operator.FileScanSocketEnabled.getStream(this, java.io.InputStream.class);
}
}
