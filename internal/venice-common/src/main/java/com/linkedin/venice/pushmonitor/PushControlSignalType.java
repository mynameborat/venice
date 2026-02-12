package com.linkedin.venice.pushmonitor;

/**
 * Types of control signals that the controller can emit for a push (version topic).
 * Servers subscribe to the ZK path for a given push and react to signals as they appear.
 *
 * <p>This is a controller→server notification mechanism, complementing the existing
 * server→controller flow in {@link OfflinePushStatus} (replica status reporting).
 */
public enum PushControlSignalType {
  /**
   * Indicates that blob data upload is complete for a blob-based push.
   * Emitted by the controller when VPJ signals that all SST files have been uploaded
   * to blob storage. Servers watching this signal should begin fetching SST files.
   */
  BLOB_UPLOAD_COMPLETE
}
