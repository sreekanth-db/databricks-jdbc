package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ChunkInfo;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.google.common.collect.ImmutableMap;

import java.sql.SQLException;

class ArrowStreamResult implements IExecutionResult {

  private final long totalRows;
  private final long totalChunks;

  private final IDatabricksSession session;
  private final ImmutableMap<Long, ChunkInfo> rowOffsetToChunkMap;
  private final ChunkDownloader chunkDownloader;

  private long currentRowIndex;
  private int currentChunkIndex;

  private boolean firstChunkPopulated;

  private ArrowResultChunk.ArrowResultChunkIterator chunkIterator;

  ArrowStreamResult(ResultManifest resultManifest, ResultData resultData, String statementId,
                    IDatabricksSession session) {
    this.totalRows = resultManifest.getTotalRowCount();
    this.totalChunks = resultManifest.getTotalChunkCount();
    this.rowOffsetToChunkMap = getRowOffsetMap(resultManifest);
    // Initialize to before first row
    this.currentRowIndex = -1;
    this.session = session;
    this.chunkDownloader = new ChunkDownloader(statementId, resultManifest, resultData, session);
    this.firstChunkPopulated = false;
    this.currentRowIndex = -1;
  }

  public ChunkDownloader getChunkDownloader() {return this.chunkDownloader;}

  private static ImmutableMap<Long, ChunkInfo> getRowOffsetMap(ResultManifest resultManifest) {
    ImmutableMap.Builder<Long, ChunkInfo> rowOffsetMapBuilder = ImmutableMap.builder();
    for (ChunkInfo chunk : resultManifest.getChunks()) {
      rowOffsetMapBuilder.put(chunk.getRowOffset(), chunk);
    }
    return rowOffsetMapBuilder.build();
  }
  
  @Override
  public Object getObject(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getCurrentRow() {
    return this.currentRowIndex;
  }

  @Override
  public boolean next() {
    if(!this.firstChunkPopulated) {
      // get first chunk from chunk downloader and set iterator to its iterator i.e. row 0
      if(this.totalChunks == 0) return false;
      ++this.currentRowIndex;
      ArrowResultChunk firstChunk = this.chunkDownloader.getChunk(/*chunkIndex =*/ 0L);
      this.chunkIterator = firstChunk.getChunkIterator();
      this.firstChunkPopulated = true;
      return true;
    }
    ++this.currentRowIndex;
    if(this.chunkIterator.nextRow()) {
      return true;
    }
    // switch to next chunk and iterate over it
    if(++this.currentChunkIndex == this.totalChunks) return false; // this implies that this was the last chunk
    ArrowResultChunk nextChunk = this.chunkDownloader.getChunk(this.currentChunkIndex);
    this.chunkIterator = nextChunk.getChunkIterator();
    return true;
  }

  @Override
  public boolean hasNext() {
    return ((this.currentChunkIndex < (totalChunks - 1)) ||
            ((currentChunkIndex == (totalChunks - 1)) && chunkIterator.hasNextRow()));
  }
}
