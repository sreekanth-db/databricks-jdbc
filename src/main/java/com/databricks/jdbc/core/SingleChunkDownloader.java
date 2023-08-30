package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.IDatabricksHttpClient;

import java.util.concurrent.Callable;

public class SingleChunkDownloader implements Callable<Void> {

  private final ArrowResultChunk chunk;
  private final IDatabricksHttpClient httpClient;
  private final IDatabricksSession session;

  SingleChunkDownloader(ArrowResultChunk chunk, IDatabricksHttpClient httpClient, IDatabricksSession session) {
    this.chunk = chunk;
    this.httpClient = httpClient;
    this.session = session;
  }

  @Override
  public Void call() throws Exception {
    if (!chunk.isChunkLinkValid()) {
      chunk.refreshChunkLink(session);
    }
    if (chunk.enterLock()) {
      try {
        chunk.downloadData(httpClient);
        chunk.releaseLock(true /* downloadSuccess */);
      } catch (DatabricksHttpException | DatabricksParsingException e) {
        // TODO: handle retries
        chunk.releaseLock(false /* downloadSuccess */);
      }
    }
    return null;
  }


}
