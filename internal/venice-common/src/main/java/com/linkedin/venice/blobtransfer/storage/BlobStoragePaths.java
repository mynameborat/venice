package com.linkedin.venice.blobtransfer.storage;

/**
 * Static helper for building well-known blob storage paths.
 *
 * Path convention:
 *   {baseUri}/{storeName}/v{version}/
 *   {baseUri}/{storeName}/v{version}/p{partition}/
 *   {baseUri}/{storeName}/v{version}/p{partition}/data_0.sst
 *   {baseUri}/{storeName}/v{version}/p{partition}/manifest.json
 *   {baseUri}/{storeName}/v{version}/manifest.json
 */
public final class BlobStoragePaths {
  private BlobStoragePaths() {
  }

  public static String versionDir(String baseUri, String storeName, int version) {
    return baseUri + "/" + storeName + "/v" + version;
  }

  public static String partitionDir(String baseUri, String storeName, int version, int partition) {
    return versionDir(baseUri, storeName, version) + "/p" + partition;
  }

  public static String sstFile(String baseUri, String storeName, int version, int partition, String fileName) {
    return partitionDir(baseUri, storeName, version, partition) + "/" + fileName;
  }

  public static String partitionManifest(String baseUri, String storeName, int version, int partition) {
    return partitionDir(baseUri, storeName, version, partition) + "/manifest.json";
  }

  public static String versionManifest(String baseUri, String storeName, int version) {
    return versionDir(baseUri, storeName, version) + "/manifest.json";
  }
}
