package com.linkedin.venice.blobtransfer.storage;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;


/**
 * Local-filesystem implementation of {@link BlobStorageClient}.
 * Useful for integration tests and single-node development.
 */
public class LocalFsBlobStorageClient implements BlobStorageClient {
  @Override
  public BlobTransferResult upload(String localPath, String remotePath) throws IOException {
    Path src = Paths.get(localPath);
    Path dst = Paths.get(remotePath);
    Files.createDirectories(dst.getParent());
    Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
    long bytes = Files.size(dst);
    return new BlobTransferResult(remotePath, bytes, true, null);
  }

  @Override
  public BlobTransferResult download(String remotePath, String localPath) throws IOException {
    Path src = Paths.get(remotePath);
    Path dst = Paths.get(localPath);
    if (Files.isDirectory(src)) {
      // Copy all files from the source directory into the destination directory
      Files.createDirectories(dst);
      long[] totalBytes = { 0 };
      Files.walkFileTree(src, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Path relative = src.relativize(file);
          Path target = dst.resolve(relative);
          Files.createDirectories(target.getParent());
          Files.copy(file, target, StandardCopyOption.REPLACE_EXISTING);
          totalBytes[0] += Files.size(target);
          return FileVisitResult.CONTINUE;
        }
      });
      return new BlobTransferResult(localPath, totalBytes[0], true, null);
    } else {
      Files.createDirectories(dst.getParent());
      Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
      long bytes = Files.size(dst);
      return new BlobTransferResult(localPath, bytes, true, null);
    }
  }

  @Override
  public boolean exists(String remotePath) throws IOException {
    return Files.exists(Paths.get(remotePath));
  }

  @Override
  public void delete(String remotePath) throws IOException {
    Files.deleteIfExists(Paths.get(remotePath));
  }

  @Override
  public void deleteDirectory(String remotePath) throws IOException {
    Path dir = Paths.get(remotePath);
    if (!Files.exists(dir)) {
      return;
    }
    Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        if (exc != null) {
          throw exc;
        }
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  @Override
  public void close() {
    // No resources to release for local filesystem
  }
}
