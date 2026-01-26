package com.linkedin.venice.blobtransfer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Manifest file for a blob-based push of a single partition.
 * This contains all metadata needed for servers to pull and ingest SST files.
 *
 * Example format:
 * {
 *   "storeName": "my_store",
 *   "version": 5,
 *   "partition": 0,
 *   "sstFiles": [
 *     {"fileName": "000001.sst", "fileSize": 1234567, "checksum": "abc123", "columnFamily": "DEFAULT"},
 *     {"fileName": "000002.sst", "fileSize": 987654, "checksum": "def456", "columnFamily": "REPLICATION_METADATA"}
 *   ],
 *   "recordCount": 1000000,
 *   "tableFormat": "BLOCK_BASED_TABLE",
 *   "rocksDbOptions": { ... }
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BlobTransferManifest {
  public static final String MANIFEST_FILE_NAME = "manifest.json";
  public static final String DEFAULT_COLUMN_FAMILY = "DEFAULT";
  public static final String REPLICATION_METADATA_COLUMN_FAMILY = "REPLICATION_METADATA";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String storeName;
  private final int version;
  private final int partition;
  private final List<BlobFileMetadata> sstFiles;
  private final long recordCount;
  private final String tableFormat;
  private final Map<String, String> rocksDbOptions;

  @JsonCreator
  public BlobTransferManifest(
      @JsonProperty("storeName") String storeName,
      @JsonProperty("version") int version,
      @JsonProperty("partition") int partition,
      @JsonProperty("sstFiles") List<BlobFileMetadata> sstFiles,
      @JsonProperty("recordCount") long recordCount,
      @JsonProperty("tableFormat") String tableFormat,
      @JsonProperty("rocksDbOptions") Map<String, String> rocksDbOptions) {
    this.storeName = storeName;
    this.version = version;
    this.partition = partition;
    this.sstFiles = sstFiles != null ? sstFiles : Collections.emptyList();
    this.recordCount = recordCount;
    this.tableFormat = tableFormat;
    this.rocksDbOptions = rocksDbOptions != null ? rocksDbOptions : Collections.emptyMap();
  }

  public String getStoreName() {
    return storeName;
  }

  public int getVersion() {
    return version;
  }

  public int getPartition() {
    return partition;
  }

  public List<BlobFileMetadata> getSstFiles() {
    return sstFiles;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public String getTableFormat() {
    return tableFormat;
  }

  public Map<String, String> getRocksDbOptions() {
    return rocksDbOptions;
  }

  /**
   * Serialize this manifest to a JSON string.
   */
  public String toJson() {
    try {
      return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new VeniceException("Failed to serialize BlobTransferManifest to JSON", e);
    }
  }

  /**
   * Deserialize a manifest from a JSON string.
   */
  public static BlobTransferManifest fromJson(String json) {
    try {
      return OBJECT_MAPPER.readValue(json, BlobTransferManifest.class);
    } catch (JsonProcessingException e) {
      throw new VeniceException("Failed to deserialize BlobTransferManifest from JSON", e);
    }
  }

  /**
   * Deserialize a manifest from an InputStream.
   */
  public static BlobTransferManifest fromInputStream(InputStream inputStream) {
    try {
      return OBJECT_MAPPER.readValue(inputStream, BlobTransferManifest.class);
    } catch (IOException e) {
      throw new VeniceException("Failed to deserialize BlobTransferManifest from InputStream", e);
    }
  }

  @Override
  public String toString() {
    return "BlobTransferManifest{" + "storeName='" + storeName + '\'' + ", version=" + version + ", partition="
        + partition + ", sstFiles=" + sstFiles + ", recordCount=" + recordCount + ", tableFormat='" + tableFormat + '\''
        + '}';
  }
}
