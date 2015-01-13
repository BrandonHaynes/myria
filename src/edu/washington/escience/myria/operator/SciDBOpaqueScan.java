package edu.washington.escience.myria.operator;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.LittleEndianDataInputStream;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

import edu.washington.escience.myria.util.SciDBChunk;
import edu.washington.escience.myria.util.SciDBHeader;

public class SciDBOpaqueScan extends LeafOperator {
  private static final String ARRAY_PREAMBLE = "\\u0000\\u0022";
  private static final String ARRAY_ARCHIVE_TYPE = "serialization::archive";
  private static final int ARCHIVE_VERSION = 10;
  private static final int HEADER_MAGIC = 0x5AC00E;
  private static final int HEADER_VERSION = 1;
  private static final byte HEADER_SPARSE_FLAG = 1;
  private static final byte HEADER_RLE_FLAG = 2;
  private static final byte HEADER_COORDINATE_MAPPING = 4;  
  private static final byte HEADER_METADATA_FLAG = 8;
  private static final int COORDINATE_SIZE = 8; // Number of bytes in Coordinate typedef (int64_t)

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The schema for the relation stored in this file. */
  protected final Schema schema;
  /** The source of the input data. */
  private final DataSource source;
  /** Holds the tuples that are ready for release. */
  protected transient TupleBatchBuffer buffer;
  /** Indicates the endianess of the bin file to read. */
  protected final boolean isLittleEndian;
  /** Data input to read data from the bin file. */
  protected transient DataInput dataInput;
  /** Data input to read data from the bin file. */
  private transient SciDBHeader header;


  public SciDBOpaqueScan(final Schema schema, final DataSource source, final boolean isLittleEndian) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.source = Objects.requireNonNull(source, "source");
    this.isLittleEndian = isLittleEndian;
  }

  public SciDBOpaqueScan(final Schema schema, final DataSource source) {
    this(schema, source, false);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    int index = 0;
    int dimensions = header.getDimensions().size();

    while (buffer.numTuples() < TupleBatch.BATCH_SIZE) {
      try {
        for (index = dimensions; index < schema.numColumns(); index++) {
          switch (schema.getColumnType(index)) {
            case DOUBLE_TYPE:
              buffer.putDouble(index, dataInput.readDouble());
              break;
            case FLOAT_TYPE:
              buffer.putFloat(index, dataInput.readFloat());
              break;
            case INT_TYPE:
              buffer.putInt(index, dataInput.readInt());
              break;
            case LONG_TYPE:
              buffer.putLong(index, dataInput.readLong());
              break;
            default:
              throw new UnsupportedOperationException(
                  "Reading variable width fields not currently supported.");
          }
        } 
      } catch (EOFException e) {
        if (index != schema.numColumns() - 1)
          throw new DbException("Ran out of data in the middle of a row", e);
        break;
      } catch (IOException e) {
        throw new DbException(e);
      }

    for(index = 0; index < dimensions; index++)
      buffer.putLong(index, -1);
    }

    return buffer.popAny();
  }

  @Override
  protected final void cleanup() throws DbException {
    while (buffer.numTuples() > 0)
      buffer.popAny();
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    try {
      InputStream inputStream = new BufferedInputStream(source.getInputStream());
      buffer = new TupleBatchBuffer(getSchema());    
      dataInput = isLittleEndian
        ? new LittleEndianDataInputStream(inputStream) 
        : new DataInputStream(inputStream);

      header = new SciDBHeader(dataInput);
      SciDBChunk chunk = new SciDBChunk(header, dataInput);
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @Override
  protected Schema generateSchema() {
    return schema;
  }
}
