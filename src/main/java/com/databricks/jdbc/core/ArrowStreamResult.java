package com.databricks.jdbc.core;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;
import com.databricks.sdk.service.sql.*;
import com.google.common.collect.ImmutableMap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class ArrowStreamResult implements IExecutionResult {

  private final IDatabricksSession session;
  private final ImmutableMap<Long, ChunkInfo> rowOffsetToChunkMap;
  private final ChunkDownloader chunkDownloader;

  private long currentRowIndex;

  private boolean isClosed;

  private ArrowResultChunk.ArrowResultChunkIterator chunkIterator;

  List<ColumnInfo> columnInfos;

  ArrowStreamResult(ResultManifest resultManifest, ResultData resultData, String statementId,
                    IDatabricksSession session) {
    this.rowOffsetToChunkMap = getRowOffsetMap(resultManifest);
    this.session = session;
    this.chunkDownloader = new ChunkDownloader(statementId, resultManifest, resultData, session);
    this.columnInfos = new ArrayList(resultManifest.getSchema().getColumns());
    this.currentRowIndex = -1;
    this.isClosed = false;
    this.chunkIterator = null;
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
    Object unconvertedObject = this.chunkIterator.getColumnObjectAtCurrentRow(columnIndex);
    return ArrowToJavaObjectConverter.convert(unconvertedObject, requiredType);
  }

  @Override
  public long getCurrentRow() {
    return this.currentRowIndex;
  }

  @Override
  public boolean next() {
    if (!hasNext()) {
      return false;
    }

    this.currentRowIndex++;
    if (this.chunkIterator == null || !this.chunkIterator.hasNextRow()) {
      this.chunkDownloader.next();
      ArrowResultChunk resultChunk = this.chunkDownloader.getChunk();
      this.chunkIterator = resultChunk.getChunkIterator();
      return true;
    }

    return this.chunkIterator.nextRow();
  }

  @Override
  public boolean hasNext() {
    return !isClosed() && ((chunkIterator != null && chunkIterator.hasNextRow())
        || chunkDownloader.hasNextChunk());
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
