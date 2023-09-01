package com.databricks.jdbc.core;

import org.apache.arrow.vector.types.Types;
import com.databricks.sdk.service.sql.*;
import com.google.common.collect.ImmutableMap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class ArrowStreamResult implements IExecutionResult {

  private final long totalRows;
  private final long totalChunks;

  private final IDatabricksSession session;
  private final ImmutableMap<Long, ChunkInfo> rowOffsetToChunkMap;
  private final ChunkDownloader chunkDownloader;

  private long currentRowIndex;
  private int currentChunkIndex;

  private boolean firstChunkPopulated;
  private boolean isClosed;

  private ArrowResultChunk.ArrowResultChunkIterator chunkIterator;

  List<ColumnInfo> columnInfos;

  ArrowStreamResult(ResultManifest resultManifest, ResultData resultData, String statementId,
                    IDatabricksSession session) {
    this.totalRows = resultManifest.getTotalRowCount();
    this.totalChunks = resultManifest.getTotalChunkCount();
    this.rowOffsetToChunkMap = getRowOffsetMap(resultManifest);
    this.session = session;
    this.chunkDownloader = new ChunkDownloader(statementId, resultManifest, resultData, session);
    this.firstChunkPopulated = false;
    this.columnInfos = new ArrayList(resultManifest.getSchema().getColumns());
    this.currentRowIndex = -1;
    this.isClosed = false;
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
    // we have two types:
    // 1. Required type via the metadata
    // 2. Interpreted type while reading from the arrow file into the record batches
    // We need to convert the interpreted type into the required type before returning the object
    ColumnInfoTypeName requiredType = columnInfos.get(columnIndex).getTypeName();
    Types.MinorType arrowType = this.chunkIterator.getColumnType(columnIndex);
    Object unconvertedObject = this.chunkIterator.getObjectAtCurrentRow(columnIndex);
    return ArrowToJavaObjectConverter.convert(unconvertedObject, requiredType, arrowType);
  }

  @Override
  public long getCurrentRow() {
    return this.currentRowIndex;
  }

  @Override
  public boolean next() {
    if (isClosed()) {
      return false;
    }
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
    return !isClosed() && ((this.currentChunkIndex < (totalChunks - 1)) ||
            ((currentChunkIndex == (totalChunks - 1)) && chunkIterator.hasNextRow()));
  }

  @Override
  public void close() {
    this.isClosed = true;
    this.chunkDownloader.releaseAllChunks();
  }

  private boolean isClosed() {
    return this.isClosed;
  }
}
