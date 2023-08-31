package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.IDatabricksHttpClient;

import java.util.concurrent.Callable;

/**
 * Task class to manage download for a single chunk.
 */
class SingleChunkDownloader implements Callable<Void> {

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
    try {
      chunk.downloadData(httpClient);
    } catch (DatabricksHttpException | DatabricksParsingException e) {
      // TODO: handle retries
    }
    return null;
  }
}
