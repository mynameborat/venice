package com.linkedin.venice.blobtransfer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link BlobTransferManifest}.
 */
public class BlobTransferManifestTest {
  @Test
  public void testManifestCreation() {
    BlobTransferManifest manifest = new BlobTransferManifest(
        "test-store",
        1,
        0,
        Collections.emptyList(),
        0,
        "BLOCK_BASED_TABLE",
        Collections.emptyMap());

    assertEquals(manifest.getStoreName(), "test-store");
    assertEquals(manifest.getVersion(), 1);
    assertEquals(manifest.getPartition(), 0);
    assertEquals(manifest.getRecordCount(), 0);
    assertNotNull(manifest.getSstFiles());
    assertTrue(manifest.getSstFiles().isEmpty());
  }

  @Test
  public void testManifestWithSstFiles() {
    List<BlobFileMetadata> sstFiles = new ArrayList<>();
    sstFiles.add(new BlobFileMetadata("000001.sst", 1024L, "abc123", BlobTransferManifest.DEFAULT_COLUMN_FAMILY));
    sstFiles.add(
        new BlobFileMetadata("000002.sst", 2048L, "def456", BlobTransferManifest.REPLICATION_METADATA_COLUMN_FAMILY));

    BlobTransferManifest manifest =
        new BlobTransferManifest("test-store", 1, 0, sstFiles, 5000, "BLOCK_BASED_TABLE", Collections.emptyMap());

    assertEquals(manifest.getSstFiles().size(), 2);
    assertEquals(manifest.getSstFiles().get(0).getFileName(), "000001.sst");
    assertEquals(manifest.getSstFiles().get(1).getFileName(), "000002.sst");
    assertEquals(manifest.getRecordCount(), 5000);
  }

  @Test
  public void testSerializationDeserialization() throws IOException {
    List<BlobFileMetadata> sstFiles = new ArrayList<>();
    sstFiles.add(new BlobFileMetadata("000001.sst", 1024L, "abc123", BlobTransferManifest.DEFAULT_COLUMN_FAMILY));
    sstFiles.add(
        new BlobFileMetadata("000002.sst", 2048L, "def456", BlobTransferManifest.REPLICATION_METADATA_COLUMN_FAMILY));

    Map<String, String> rocksDbOptions = new HashMap<>();
    rocksDbOptions.put("compression", "LZ4");

    BlobTransferManifest manifest =
        new BlobTransferManifest("test-store", 1, 0, sstFiles, 5000, "BLOCK_BASED_TABLE", rocksDbOptions);

    // Serialize
    String json = manifest.toJson();

    // Verify JSON contains expected fields (note: JSON may be pretty-printed with whitespace)
    assertTrue(json.contains("\"storeName\""));
    assertTrue(json.contains("test-store"));
    assertTrue(json.contains("\"version\""));
    assertTrue(json.contains("\"partition\""));
    assertTrue(json.contains("\"recordCount\""));
    assertTrue(json.contains("\"sstFiles\""));

    // Deserialize
    ByteArrayInputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    BlobTransferManifest deserializedManifest = BlobTransferManifest.fromInputStream(inputStream);

    assertEquals(deserializedManifest.getStoreName(), "test-store");
    assertEquals(deserializedManifest.getVersion(), 1);
    assertEquals(deserializedManifest.getPartition(), 0);
    assertEquals(deserializedManifest.getRecordCount(), 5000);
    assertEquals(deserializedManifest.getSstFiles().size(), 2);

    // Verify SST file metadata
    BlobFileMetadata deserializedMetadata1 = deserializedManifest.getSstFiles().get(0);
    assertEquals(deserializedMetadata1.getFileName(), "000001.sst");
    assertEquals(deserializedMetadata1.getFileSize(), 1024L);
    assertEquals(deserializedMetadata1.getChecksum(), "abc123");
    assertEquals(deserializedMetadata1.getColumnFamily(), BlobTransferManifest.DEFAULT_COLUMN_FAMILY);

    BlobFileMetadata deserializedMetadata2 = deserializedManifest.getSstFiles().get(1);
    assertEquals(deserializedMetadata2.getFileName(), "000002.sst");
    assertEquals(deserializedMetadata2.getColumnFamily(), BlobTransferManifest.REPLICATION_METADATA_COLUMN_FAMILY);
  }

  @Test
  public void testManifestFileName() {
    assertEquals(BlobTransferManifest.MANIFEST_FILE_NAME, "manifest.json");
  }

  @Test
  public void testColumnFamilyConstants() {
    assertEquals(BlobTransferManifest.DEFAULT_COLUMN_FAMILY, "DEFAULT");
    assertEquals(BlobTransferManifest.REPLICATION_METADATA_COLUMN_FAMILY, "REPLICATION_METADATA");
  }

  @Test
  public void testEmptyManifestSerialization() throws IOException {
    BlobTransferManifest manifest = new BlobTransferManifest(
        "empty-store",
        1,
        5,
        Collections.emptyList(),
        0,
        "BLOCK_BASED_TABLE",
        Collections.emptyMap());

    String json = manifest.toJson();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    BlobTransferManifest deserializedManifest = BlobTransferManifest.fromInputStream(inputStream);

    assertEquals(deserializedManifest.getStoreName(), "empty-store");
    assertEquals(deserializedManifest.getVersion(), 1);
    assertEquals(deserializedManifest.getPartition(), 5);
    assertTrue(deserializedManifest.getSstFiles().isEmpty());
  }
}
