package edu.washington.escience.myria.util;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class SciDBHeader {
  private static final String ARRAY_ARCHIVE_TYPE = "serialization::archive";
  private static final int ARCHIVE_VERSION = 10;
  private static final int HEADER_MAGIC = 0x5AC00E;
  private static final int HEADER_VERSION = 1;
  private static final byte HEADER_SPARSE_FLAG = 1;
  private static final byte HEADER_RLE_FLAG = 2;
  private static final byte HEADER_COORDINATE_MAPPING = 4;  
  private static final byte HEADER_METADATA_FLAG = 8;

  /* Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private final Map<String, Integer> trackedItems = new HashMap<String, Integer>();
  private final Map<String, Integer> trackedItemVersions = new HashMap<String, Integer>();

  private int id;
  private int version;
  private String name;
  private List<String> dimensions = new ArrayList<String>();
  private List<String> attributes = new ArrayList<String>();
  private boolean isSparse;
  private boolean isRLE;
  private int bodySize;

  public SciDBHeader(final DataInput dataInput) throws IOException {
    this(dataInput, true);
  }

  protected SciDBHeader(final DataInput dataInput, boolean hasMetadata) throws IOException {
    readHeader(dataInput, hasMetadata);
  }

  public int getBodySize() {
    return bodySize;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  private void readHeader(DataInput dataInput, boolean requireMetadataFlag) throws IOException {
    int magic = dataInput.readInt();
    int version = dataInput.readInt();
    bodySize = dataInput.readInt();
    int signature = dataInput.readInt();
    long attributeId = dataInput.readLong();
    byte compressionMethod = dataInput.readByte();
    byte flags = dataInput.readByte();
    byte dimensions = dataInput.readByte(); 
    boolean isSparse = (flags & HEADER_SPARSE_FLAG) != 0;
    boolean isRLE = (flags & HEADER_RLE_FLAG) != 0;

    dataInput.skipBytes(5); // Archive header

    if(magic != HEADER_MAGIC)
      throw new IOException("Unrecognized archive header magic " + magic);
    else if(version != HEADER_VERSION)
      throw new IOException("Unrecognized archive header version " + version);
    else if((flags & HEADER_COORDINATE_MAPPING) != 0)
      throw new UnsupportedOperationException("Unsupported flags " + flags);
    else if(requireMetadataFlag && (flags & HEADER_METADATA_FLAG) == 0)
      throw new UnsupportedOperationException("Metadata flag is required");

    if(requireMetadataFlag)
      readDescription(dataInput);
  }

  private void readDescription(DataInput dataInput) throws IOException {
    byte[] description = new byte[bodySize];
    Queue<String> tokens = new ArrayDeque<String>();

    dataInput.readFully(description);
    tokens.addAll(Arrays.asList(new String(description).split(" ")));

    consume(ARRAY_ARCHIVE_TYPE.length(), tokens);
    consume(ARRAY_ARCHIVE_TYPE, tokens);
    consume(ARCHIVE_VERSION, tokens);
    consumeTrackingVersion("ArrayDescription", tokens);

    id = consumeInteger(tokens);
    int uAId = consumeInteger(tokens);
    version = consumeInteger(tokens);
    name = consumeFixedLengthString(tokens);

    readAttributeDescription(tokens);
    readDimensions(tokens);

    int flags = consumeInteger(tokens);
  }

  private void readAttributeDescription(Queue<String> tokens) throws IOException {
    consumeTrackingVersion("AttributeDescriptionCollection", tokens);

    int attributeCount = consumeInteger(tokens);

    for(int index = 0; index < attributeCount; index++) {
      consumeItemVersion("AttributeBody", tokens);
      consumeTrackingVersion("AttributeBody", tokens);

      int attributeId = consumeInteger(tokens);
      attributes.add(consumeFixedLengthString(tokens));
      List<String> aliases = readAliases(tokens);
      String type = consumeFixedLengthString(tokens);
      int flags = consumeInteger(tokens);
      short defaultCompressionMethod = consumeShort(tokens);
      short reserve = consumeShort(tokens);
      String defaultValue = readValue(tokens);
      int variableSize = consumeInteger(tokens);
      String defaultValueExpression = consumeFixedLengthString(tokens);
    }
  }

  private List<String> readAliases(Queue<String> tokens) throws IOException {
    consumeTrackingVersion("AliasCollection", tokens);
    int aliasCount = consumeInteger(tokens);
    List<String> aliases = new ArrayList<String>(aliasCount);

    for(int alias = 0; alias < aliasCount; alias++) {
      consumeItemVersion(tokens);
      aliases.add(consumeFixedLengthString(tokens));
    }    

    return aliases;
  }

  private String readValue(Queue<String> tokens) throws IOException {
    consumeTrackingVersion("Value", tokens);
    int size = consumeInteger(tokens);
    int missingReason = consumeInteger(tokens);; // Actually an enum

    char[] data = new char[size];
    for(int index = 0; index < size; index++)
      data[index] = tokens.remove().charAt(0);

    boolean hasTile = consumeInteger(tokens) != 0;

    if(hasTile)
      readRLEPayload(tokens);

    return new String(data);
  }

  private void readRLEPayload(Queue<String> tokens) throws IOException {
    int totalSegments = consumeInteger(tokens);
    long elementSize = consumeLong(tokens);
    long dataSize = consumeLong(tokens);
    long offsets = consumeLong(tokens);

    // Container
    int segmentCount = consumeInteger(tokens);
    List<Integer> segments = new ArrayList<Integer>(segmentCount);

    for(int segment = 0; segment < segmentCount; segment++) {
      consumeItemVersion(tokens);
      segments.add(readSegment(tokens));
    }    

    int dataCount = consumeInteger(tokens);
    char[] data = new char[dataCount];

    for(int index = 0; index < dataCount; index++)
      data[index] = tokens.remove().charAt(0);

    boolean isBoolean = consumeBoolean(tokens);
  }

  private int readSegment(Queue<String> tokens) throws IOException {
    int position = consumeInteger(tokens);
    int valueIndex = consumeInteger(tokens);
    short sameSequence = consumeShort(tokens);
    return position;
  }

  private void readDimensions(Queue<String> tokens) throws IOException {
    consumeTrackingVersion("Dimensions", tokens);
    int dimensionCount = consumeInteger(tokens);

    for(int dimension = 0; dimension < dimensionCount; dimension++) {
      consumeItemVersion("Dimension", tokens);
      consumeTrackingVersion("Dimension", tokens);

      consumeTrackingVersion("Dimension-BaseName", tokens);
      dimensions.add(consumeFixedLengthString(tokens)); // Base Name

      readNames(tokens);

      int startMinimum = consumeInteger(tokens);
      int currentStart = consumeInteger(tokens);
      int currentEnd = consumeInteger(tokens);
      int endMaximum = consumeInteger(tokens);
      int chunkInterval = consumeInteger(tokens);
      int chunkOverlap = consumeInteger(tokens);
      }
  }

  private void readNames(Queue<String> tokens) throws IOException {
    consumeTrackingVersion("Names", tokens);
    int nameCount = consumeInteger(tokens);
  
    for(int index = 0; index < nameCount; index++) {
      consumeTrackingVersion("Names-Map", tokens);

      int keyIndex = consumeInteger(tokens);
      String name = consumeFixedLengthString(tokens);
      int valueIndex = consumeInteger(tokens);
      int valueVersion = consumeInteger(tokens); // ?
      String base = consumeFixedLengthString(tokens);
    }
  }  

  // Convenience functions 

  private String consume(String expected, Queue<String> tokens) throws IOException {
    String value = tokens.remove();
    if(!value.equals(expected))
      throw new IOException(String.format("Expected '%s', found '%s'.", expected, value));
    return value;
  }

  private int consume(int expected, Queue<String> tokens) throws IOException {
    int value = consumeInteger(tokens);
    if(value != expected)
      throw new IOException(String.format("Expected '%d', found '%d'.", expected, value));
    return value;
  }

  private boolean consumeBoolean(Queue<String> tokens) throws IOException {
    return consumeInteger(tokens) != 0;
  }

  private int consumeInteger(Queue<String> tokens) throws IOException {
    String value = tokens.remove();

    try {
      return Integer.parseInt(value);
    } catch(NumberFormatException e) {
      throw new IOException(String.format("Invalid integer '%s' encountered.", value), e);
    }
  }

  private short consumeShort(Queue<String> tokens) throws IOException {
    String value = tokens.remove();

    try {
      return Short.parseShort(value);
    } catch(NumberFormatException e) {
      throw new IOException(String.format("Invalid integer '%s' encountered.", value), e);
    }
  }

  private long consumeLong(Queue<String> tokens) throws IOException {
    String value = tokens.remove();

    try {
      return Long.parseLong(value);
    } catch(NumberFormatException e) {
      throw new IOException(String.format("Invalid long '%s' encountered.", value), e);
    }
  }

  private String consumeFixedLengthString(Queue<String> tokens) throws IOException {
    int size = consumeInteger(tokens);
    String value = tokens.remove();
    if(size < 0)
      throw new IOException(String.format("Found invalid size %d", size));
    else if(value.length() != size)
      throw new IOException(String.format("Expected '%s' to be length %d, found %d.", value, size, value.length()));
    return value;
  }

  private int consumeTrackingVersion(String name, Queue<String> tokens) throws IOException {
    if(!trackedItems.containsKey(name)) {
      int trackingLevel = consumeInteger(tokens);
      int version = consumeInteger(tokens);

      if(trackingLevel < 0)
        throw new IOException(String.format("Found invalid tracking level %d", trackingLevel));      
      else if(version < 0)
        throw new IOException(String.format("Found invalid version %d", version));

      trackedItems.put(name, version);
    }

    return trackedItems.get(name);
  }

  private int consumeItemVersion(Queue<String> tokens) throws IOException {
    int itemVersion = consumeInteger(tokens);
    if(itemVersion < 0)
      throw new IOException(String.format("Found invalid item version %d", itemVersion));

    return itemVersion;
  }

  private int consumeItemVersion(String name, Queue<String> tokens) throws IOException {
    if(!trackedItemVersions.containsKey(name))
      trackedItemVersions.put(name, consumeItemVersion(tokens));
    return trackedItemVersions.get(name);
  }
}
