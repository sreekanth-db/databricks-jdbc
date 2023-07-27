package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ChunkInfo;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class ChunkDownloader {
  private final long totalChunks;
  private long currentChunk;
  private long chunkLinkDownloadedOffset;
  private long chunkDownloadedOffset;
  private long totalBytesInUse;

  ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexToChunksMap;

  ChunkDownloader(ResultManifest resultManifest, ResultData resultData) {
    this.totalChunks = resultManifest.getTotalChunkCount();
    this.chunkIndexToChunksMap = initializeChunksMap(resultManifest, resultData);
    this.chunkLinkDownloadedOffset = resultData.getExternalLinks().size();
  }

  private static ConcurrentHashMap<Long, ArrowResultChunk> initializeChunksMap(ResultManifest resultManifest, ResultData resultData) {
    ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexMap = new ConcurrentHashMap<>();
    for (ChunkInfo chunkInfo : resultManifest.getChunks()) {
      chunkIndexMap.put(chunkInfo.getChunkIndex(), new ArrowResultChunk(chunkInfo));
    }

    for (ExternalLink externalLink : resultData.getExternalLinks()) {
      chunkIndexMap.get(externalLink.getChunkIndex()).setChunkUrl(externalLink);
    }
    return chunkIndexMap;
  }

  /**
   * Fetches the chunk for the given index. If chunk is not already downloaded, will download the chunk first
   * @param chunkIndex index of chunk
   * @return the chunk at given index
   */
  public ArrowResultChunk getChunk(int chunkIndex) {
    // TODO: download the chunk if not already downloaded
    return chunkIndexToChunksMap.get(chunkIndex);
  }

  public void initLinksDownload(ExecutorService executorService) {
    while ()
  }

  /**
   * Starts the chunk download from given already downloaded position
   */
  public void initChunkDownload() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Release the memory for given chunk since it is already consumed
   * @param chunkIndex index of consumed chunk
   */
  public void releaseChunk(int chunkIndex) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void setChunkLink(int chunkIndex, String chunkLink) {
    chunkIndexToChunksMap.get(chunkIndex).setChunkUrl(chunkLink);
  }
}
