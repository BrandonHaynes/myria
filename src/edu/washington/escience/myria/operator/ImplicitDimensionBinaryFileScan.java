package edu.washington.escience.myria.operator;

import java.io.IOException;
import java.io.EOFException;

import java.util.List;
import java.util.ArrayList;
import java.util.Objects;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.storage.TupleBatch;

public final class ImplicitDimensionBinaryFileScan extends BinaryFileScan {
  private static final long serialVersionUID = 1L;

  private final List<Integer> dimensions;
  private final List<Integer> products = new ArrayList<>();
  private long totalTuples = 0;

  public ImplicitDimensionBinaryFileScan(final Schema schema, final DataSource source, 
                                         final List<Integer> dimensions,
                                         final boolean isLittleEndian) {
    super(schema, source, isLittleEndian);

    Objects.requireNonNull(dimensions, "dimensions");

    for(int dimension: dimensions)
      if(dimension < 0)
        throw new IllegalArgumentException("All dimensions must be non-negative.");

    this.dimensions = dimensions;

    // List of dimension products such that products[n] is the product of all dimensions 0..n-1
    products.add(1);
    for(int dimension: dimensions)
      products.add(products.get(products.size()-1) * dimension);
  }

  public ImplicitDimensionBinaryFileScan(final Schema schema, final DataSource source, List<Integer> dimensions) {
    this(schema, source, dimensions, false);
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    boolean building = false;
    
    try {
      while (buffer.numTuples() < TupleBatch.BATCH_SIZE) {
        for (int count = dimensions.size(); count < schema.numColumns(); ++count) {
            switch (schema.getColumnType(count)) {
              case DOUBLE_TYPE:
                buffer.putDouble(count, dataInput.readDouble());
                break;
              case FLOAT_TYPE:
                float readFloat = dataInput.readFloat();
                buffer.putFloat(count, readFloat);
                break;
              case INT_TYPE:
                buffer.putInt(count, dataInput.readInt());
                break;
              case LONG_TYPE:
                long readLong = dataInput.readLong();
                buffer.putLong(count, readLong);
                break;
              default:
                throw new UnsupportedOperationException(
                    "BinaryFileScan only support reading fixed width type from the binary file.");
            }
          building = true;
        }
        building = false;

        for(int index = 0; index < dimensions.size(); index++)
          buffer.putLong(index, (totalTuples / products.get(index)) % dimensions.get(index));

        totalTuples++;
      }
    } catch (EOFException e) {
      if (building)
        throw new DbException("Ran out of binary data in the middle of a row", e);
    } catch (IOException e) {
      throw new DbException(e);
    }

    return buffer.popAny();
  }
}
