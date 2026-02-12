package com.linkedin.venice.blobtransfer.storage;

/**
 * Immutable result of a blob transfer operation (upload or download).
 */
public class BlobTransferResult {
  private final String path;
  private final long bytesCopied;
  private final boolean success;
  private final String errorMessage;

  public BlobTransferResult(String path, long bytesCopied, boolean success, String errorMessage) {
    this.path = path;
    this.bytesCopied = bytesCopied;
    this.success = success;
    this.errorMessage = errorMessage;
  }

  public String getPath() {
    return path;
  }

  public long getBytesCopied() {
    return bytesCopied;
  }

  public boolean isSuccess() {
    return success;
  }

  public String getErrorMessage() {
    return errorMessage;
  }
}
