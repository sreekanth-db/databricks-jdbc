package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ChunkInfo;
import com.databricks.sdk.service.sql.ExternalLink;

import java.text.SimpleDateFormat;
import java.time.Instant;

public class ArrowResultChunk {

  /**
   * The status of a chunk would proceed in following path:
   * <ul>
   *   <li>Create placeholder for chunk, along with the chunk cardinal</li>
   *   <li>Fetch chunk url</li>
   *   <li>Submit task for data download</li>
   *   <ul>
   *     <li>Download has completed</li>
   *     <li>Download has failed and we will retry</li>
   *     <li>Download has failed and we gave up</li>
   *   </ul>
   *   <li>Data has been consumed and chunk is free to be released from memory</li>
   * </ul>
   * ->
   */
  enum DownloadStatus {
    // This is a placeholder for chunk, we don't even have the Url
    PENDING,
    // We have the Url for the chunk, and it is ready for download
    URL_FETCHED,
    // Download task has been submitted
    DOWNLOAD_IN_PROGRESS,
    // Data has been downloaded and ready for consumption
    DOWNLOAD_SUCCEEDED,
    // Download has failed and it would be retried
    DOWNLOAD_FAILED_RETRYABLE,
    // Download has failed and we have given up
    DOWNLOAD_FAILED_ABORTED,
    // Download has been cancelled
    CANCELLED,
    // Chunk has been consumed, and is free to be released. Since we do not support backward scroll in result set,
    // the chunk won't be needed again
    CHUNK_CONSUMED,
    // Chunk memory has been released
    CHUNK_RELEASED;
  }

  private final long chunkIndex;
  private final long numRows;
  private final long rowOffset;
  private final long byteCount;

  private String chunkUrl;
  private Long nextChunkIndex;
  private Instant expiryTime;
  private DownloadStatus status;

  ArrowResultChunk(ChunkInfo chunkInfo) {
    this.chunkIndex = chunkInfo.getChunkIndex();
    this.numRows = chunkInfo.getRowCount();
    this.rowOffset = chunkInfo.getRowOffset();
    this.nextChunkIndex = chunkInfo.getNextChunkIndex();
    this.byteCount = chunkInfo.getByteCount();
    this.status = DownloadStatus.PENDING;
    this.chunkUrl = null;
  }

  void setChunkUrl(ExternalLink chunk) {
    this.chunkUrl = chunk.getExternalLink();
    this.nextChunkIndex = chunk.getNextChunkIndex();
    this.expiryTime = Instant.parse(chunk.getExpiration());
    this.status = DownloadStatus.URL_FETCHED;
  }

  void setStatus(DownloadStatus status) {
    this.status = status;
  }

  boolean isChunkLinkExpired() {
    return expiryTime != null && expiryTime.isAfter(Instant.now());
  }

  DownloadStatus getStatus() {
    return this.status;
  }
}
