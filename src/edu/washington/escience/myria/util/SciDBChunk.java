package edu.washington.escience.myria.util;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SciDBChunk {
  private static final int CHUNK_PREAMBLE_SIZE = 36;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private int dataSize;
  private List<Long> coordinates = new ArrayList<Long>();

  public SciDBChunk(final SciDBHeader header, 
                    final DataInput dataInput) throws IOException {
    readHeader(header, dataInput);
  }

  private void readHeader(SciDBHeader header,
                          DataInput dataInput) throws IOException {
    SciDBHeader chunkHeader = new SciDBHeader(dataInput, false);
    int chunkSize = chunkHeader.getBodySize();
    int preambleSize = CHUNK_PREAMBLE_SIZE * header.getDimensions().size();
    dataSize = chunkSize - preambleSize;

     for(int index = 0; index < header.getDimensions().size(); index++)
      coordinates.add(dataInput.readLong());

    dataInput.skipBytes(preambleSize);
  }
}
