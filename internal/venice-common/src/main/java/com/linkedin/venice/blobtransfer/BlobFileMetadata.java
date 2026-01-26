package com.linkedin.venice.blobtransfer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Metadata for an individual blob file (SST file) in blob transfer.
 */
public class BlobFileMetadata {
  private final String fileName;
  private final long fileSize;
  private final String checksum;
  private final String columnFamily;

  @JsonCreator
  public BlobFileMetadata(
      @JsonProperty("fileName") String fileName,
      @JsonProperty("fileSize") long fileSize,
      @JsonProperty("checksum") String checksum,
      @JsonProperty("columnFamily") String columnFamily) {
    this.fileName = fileName;
    this.fileSize = fileSize;
    this.checksum = checksum;
    this.columnFamily = columnFamily;
  }

  public String getFileName() {
    return fileName;
  }

  public long getFileSize() {
    return fileSize;
  }

  public String getChecksum() {
    return checksum;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  @Override
  public String toString() {
    return "BlobFileMetadata{" + "fileName='" + fileName + '\'' + ", fileSize=" + fileSize + ", checksum='" + checksum
        + '\'' + ", columnFamily='" + columnFamily + '\'' + '}';
  }
}
