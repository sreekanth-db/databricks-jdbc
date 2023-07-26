package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ChunkInfo;
import com.databricks.sdk.service.sql.ResultManifest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkDownloader {
  private final long totalChunks;
  private int currentChunk;
  private int totalBytesInUse;

  ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexToChunksMap;

  ChunkDownloader(ResultManifest resultManifest) {
    this.totalChunks = resultManifest.getTotalChunkCount();
    this.chunkIndexToChunksMap = initializeChunksMap(resultManifest);
  }

  private static ConcurrentHashMap<Long, ArrowResultChunk> initializeChunksMap(ResultManifest resultManifest) {
    ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexMap = new ConcurrentHashMap<>();
    for (ChunkInfo chunkInfo : resultManifest.getChunks()) {
      chunkIndexMap.put(chunkInfo.getChunkIndex(), new ArrowResultChunk(chunkInfo));
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
}
