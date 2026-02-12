package com.linkedin.venice.blobtransfer.storage;

/**
 * Enum representing supported blob storage backends.
 */
public enum BlobStorageType {
  HDFS, S3, LOCAL_FS;

  /**
   * Case-insensitive factory method.
   * @param value the string representation of the blob storage type
   * @return the corresponding BlobStorageType
   * @throws IllegalArgumentException if the value does not match any type
   */
  public static BlobStorageType fromString(String value) {
    return valueOf(value.toUpperCase());
  }
}
