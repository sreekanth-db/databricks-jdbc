package com.databricks.jdbc.core;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.impl.DatabricksSdkClient;
import com.databricks.jdbc.core.ArrowResultChunk.DownloadStatus;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.service.sql.*;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ChunkDownloaderTest {

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String SESSION_ID = "session_id";
  private static final String STATEMENT_ID = "statement_id";
  private static final String STATEMENT = "select 1";
  private static final String JDBC_URL = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final String CHUNK_URL_PREFIX = "chunk.databricks.com/";
  private static final long TOTAL_CHUNKS = 5;
  private static final long TOTAL_ROWS = 90;
  private static final long TOTAL_BYTES = 1000;

  @Mock
  StatementExecutionService statementExecutionService;

  @Mock
  IDatabricksHttpClient mockHttpClient;
  @Mock
  CloseableHttpResponse httpResponse;
  @Mock
  HttpEntity httpEntity;
  @Captor
  ArgumentCaptor<HttpUriRequest> httpRequestCaptor;

  @Test
  public void testInitChunkDownloader() throws Exception {
    List<ChunkInfo> chunks = new ArrayList<>();
    for (long chunkIndex =0; chunkIndex < TOTAL_CHUNKS; chunkIndex++) {
      ChunkInfo chunkInfo = new ChunkInfo()
          .setChunkIndex(chunkIndex)
          .setByteCount(200L)
          .setRowOffset(chunkIndex*20);
      if (chunkIndex < TOTAL_CHUNKS -1) {
        chunkInfo.setNextChunkIndex(chunkIndex +1)
            .setRowCount(20L);
      } else {
        chunkInfo.setRowCount(10L);
      }
      chunks.add(chunkInfo);
    }
    ResultManifest resultManifest = new ResultManifest()
        .setTotalChunkCount(TOTAL_CHUNKS)
        .setTotalRowCount(TOTAL_ROWS)
        .setTotalByteCount(TOTAL_BYTES)
        .setChunks(chunks)
        .setSchema(new ResultSchema().setColumns(new ArrayList<>()));

    ResultData resultData = new ResultData()
        .setExternalLinks(getChunkLinks(0L, false));

    IDatabricksConnectionContext connectionContext = DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSession session = new DatabricksSession(connectionContext,
        new DatabricksSdkClient(connectionContext, statementExecutionService, null));

    when(statementExecutionService.getStatementResultChunkN(getChunkNRequest(1L)))
        .thenReturn(new ResultData().setExternalLinks(getChunkLinks(1L, false)));
    when(statementExecutionService.getStatementResultChunkN(getChunkNRequest(2L)))
        .thenReturn(new ResultData().setExternalLinks(getChunkLinks(2L, false)));
    when(statementExecutionService.getStatementResultChunkN(getChunkNRequest(3L)))
        .thenReturn(new ResultData().setExternalLinks(getChunkLinks(3L, false)));
    when(statementExecutionService.getStatementResultChunkN(getChunkNRequest(4L)))
        .thenReturn(new ResultData().setExternalLinks(getChunkLinks(4L, false)));

    setupMockResponse();
    when(mockHttpClient.execute(isA(HttpUriRequest.class))).thenReturn(httpResponse);

    ChunkDownloader chunkDownloader =
        new ChunkDownloader(STATEMENT_ID, resultManifest, resultData, session, mockHttpClient);
    verify(statementExecutionService, times(4))
        .getStatementResultChunkN(Mockito.isA(GetStatementResultChunkNRequest.class));
    // TDOD: assert urls as well
    verify(mockHttpClient, times(4)).execute(isA(HttpUriRequest.class));

    assertEquals(4, chunkDownloader.getTotalChunksInMemory());

    for (long chunkResultIndex = 0L; chunkResultIndex < TOTAL_CHUNKS; chunkResultIndex++) {
      assertChunkResult(chunkDownloader.getChunk(chunkResultIndex), chunkResultIndex);
    }
  }

  private void setupMockResponse() throws Exception {
    when(httpResponse.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent()).thenReturn(new FakeInputStream());
  }

  private List<ExternalLink> getChunkLinks(long chunkIndex, boolean isLast) {
    List<ExternalLink> chunkLinks = new ArrayList<>();
    ExternalLink chunkLink = new ExternalLink()
        .setChunkIndex(chunkIndex)
        .setExternalLink(CHUNK_URL_PREFIX + chunkIndex)
        .setExpiration(Instant.now().plusSeconds(3600L).toString());
    if (!isLast) {
      chunkLink.setNextChunkIndex(chunkIndex + 1);
    }
    chunkLinks.add(chunkLink);
    return chunkLinks;
  }

  private GetStatementResultChunkNRequest getChunkNRequest(long chunkIndex) {
    return new GetStatementResultChunkNRequest()
        .setStatementId(STATEMENT_ID)
        .setChunkIndex(chunkIndex);
  }

  private void assertChunkResult(ArrowResultChunk chunk, long chunkIndex) {
    long expectedRows = chunkIndex < 4 ? 20L : 10L;
    long expectedRowsOffSet = chunkIndex * 20L;
    assertEquals(expectedRows, chunk.numRows);
    assertEquals(expectedRowsOffSet, chunk.rowOffset);
    assertEquals(CHUNK_URL_PREFIX + chunkIndex, chunk.getChunkUrl());

    if (chunkIndex < 4) {
      assertNotNull(chunk.getDownloadFinishTime());
      assertEquals(DownloadStatus.DOWNLOAD_SUCCEEDED, chunk.getStatus());
    } else {
      assertNull(chunk.getDownloadFinishTime());
      assertEquals(DownloadStatus.URL_FETCHED, chunk.getStatus());
    }
  }

  class FakeInputStream extends InputStream {
    @Override
    public int read() throws IOException {
      return 0;
    }
  }
}
