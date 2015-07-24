package edu.washington.escience.myria.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A data source that pulls data from a specified URI. The URI may be: a path on the local file system; an HDFS link; a
 * web link; an AWS link; and perhaps more.
 * 
 * If the URI points to a directory, all files in that directory will be concatenated into a single {@link InputStream}.
 */
public class UriSourceSocketEnabled implements DataSourceSocketEnabled, Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The logger for debug, trace, etc. messages in this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(UriSourceSocketEnabled.class);

  /** The Uniform Resource Indicator (URI) of the data source. */
  @JsonProperty
  private final String uri;

  /**
   * Construct a source of data from the specified URI. The URI may be: a path on the local file system; an HDFS link; a
   * web link; an AWS link; and perhaps more.
   * 
   * If the URI points to a directory in HDFS, all files in that directory will be concatenated into a single
   * {@link InputStream}.
   * 
   * @param uri the Uniform Resource Indicator (URI) of the data source.
   */
  @JsonCreator
  public UriSourceSocketEnabled(@JsonProperty(value = "uri", required = true) final String uri) {
    this.uri = Objects.requireNonNull(uri, "Parameter uri to UriSourceSocketEnabled may not be null");
  }

  @Override
  public InputStream getInputStream() throws IOException {
return edu.washington.escience.myria.operator.FileScanSocketEnabled.getStream(this, java.io.InputStream.class);
}
}
