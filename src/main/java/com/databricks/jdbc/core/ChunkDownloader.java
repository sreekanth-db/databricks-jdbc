package com.databricks.jdbc.core;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.sdk.service.sql.ChunkInfo;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.memory.RootAllocator;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Class to manage Arrow chunks and fetch them on proactive basis.
 */
public class ChunkDownloader implements Runnable {

  private static final int CHUNKS_DOWNLOADER_THREAD_POOL_SIZE = 4;
  private static final String CHUNKS_DOWNLOADER_THREAD_POOL_PREFIX = "databricks-jdbc-chunks-downloader-";
  private final IDatabricksSession session;
  private final String statementId;
  private final long totalChunks;
  private final ExecutorService chunkDownloaderExecutorService;
  private final IDatabricksHttpClient httpClient;
  private long currentChunk;
  private long nextChunkLinkToDownload;
  private long nextChunkToDownload;
  private long totalChunksInMemory;
  private long allowedChunksInMemory;
  private long totalBytesInUse;
  private boolean isClosed;
  private Object downloadMonitor = new Object();

  ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexToChunksMap;

  ChunkDownloader(String statementId, ResultManifest resultManifest, ResultData resultData, IDatabricksSession session) {
    this(statementId, resultManifest, resultData, session, DatabricksHttpClient.getInstance());
  }

  @VisibleForTesting
  ChunkDownloader(String statementId, ResultManifest resultManifest, ResultData resultData, IDatabricksSession session, IDatabricksHttpClient httpClient) {
    this.session = session;
    this.statementId = statementId;
    this.totalChunks = resultManifest.getTotalChunkCount();
    this.chunkIndexToChunksMap = initializeChunksMap(resultManifest, resultData, statementId);
    // We are assuming that all links provided are in sequence. Currently, it only returns the
    // first chunk link, and next links need to be fetched sequentially using a separate API
    this.nextChunkLinkToDownload = resultData.getExternalLinks().size();
    // No chunks are downloaded, we need to start from first one
    this.nextChunkToDownload = 0;
    // Initialize current chunk to -1, since we don't have anything to read
    this.currentChunk = -1;
    // We don't have any chunk in downloaded yet
    this.totalChunksInMemory = 0;
    // Number of worker threads are directly linked to allowed chunks in memory
    this.allowedChunksInMemory = Math.min(CHUNKS_DOWNLOADER_THREAD_POOL_SIZE, totalChunks);
    this.chunkDownloaderExecutorService = createChunksDownloaderExecutorService();
    this.httpClient = httpClient;
    this.isClosed = false;
    // The first link is available
    this.initDownloader();
  }

  private static ConcurrentHashMap<Long, ArrowResultChunk> initializeChunksMap(ResultManifest resultManifest, ResultData resultData, String statementId) {
    ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexMap = new ConcurrentHashMap<>();
    for (ChunkInfo chunkInfo : resultManifest.getChunks()) {
      // TODO: Add logging to check data (in bytes) from server and in root allocator. If they are close, we can directly assign the number of bytes as the limit with a small buffer.
      chunkIndexMap.put(chunkInfo.getChunkIndex(), new ArrowResultChunk(chunkInfo, new RootAllocator(/*limit =*/ Integer.MAX_VALUE), statementId));
    }

    for (ExternalLink externalLink : resultData.getExternalLinks()) {
      chunkIndexMap.get(externalLink.getChunkIndex()).setChunkUrl(externalLink);
    }
    return chunkIndexMap;
  }

  private static ExecutorService createChunksDownloaderExecutorService() {
    ThreadFactory threadFactory =
        new ThreadFactory() {
          private int threadCount = 1;

          public Thread newThread(final Runnable r) {
            final Thread thread = new Thread(r);
            thread.setName(CHUNKS_DOWNLOADER_THREAD_POOL_PREFIX + threadCount++);
            // TODO: catch uncaught exceptions
            thread.setDaemon(true);

            return thread;
          }
        };
    return Executors.newFixedThreadPool(CHUNKS_DOWNLOADER_THREAD_POOL_SIZE, threadFactory);
  }

    /**
     * Fetches the chunk for the given index. If chunk is not already downloaded, will download the chunk first
     * @param chunkIndex index of chunk
     * @return the chunk at given index
     */
  public ArrowResultChunk getChunk(long chunkIndex) {
    // TODO: download the chunk if not already downloaded
    return chunkIndexToChunksMap.get(chunkIndex);
  }

  private void initDownloader() {
    // Download all links using session's executor service. The links are downloaded sequentially.
    initLinksDownload(this.session.getExecutorService());
    // We already have the first link, start the download for arrow data
    new Thread(this, "chunk-downloader-" + statementId).start();
  }

  private void initLinksDownload(ExecutorService executorService) {
    if (nextChunkLinkToDownload < totalChunks) {
      executorService.submit(
          () -> {
            while (nextChunkLinkToDownload < totalChunks) {
              // TODO: handle failures
              Collection<ExternalLink> chunks = session.getDatabricksClient().getResultChunks(
                  statementId, nextChunkLinkToDownload);
              for (ExternalLink chunkLink : chunks) {
                setChunkLink(chunkLink.getChunkIndex(), chunkLink);
                nextChunkLinkToDownload++;
              }
            }
            // Once all chunk links has been downloaded, trigger the chunks download.
            downloadMonitor.notify();
          });
    }
  }

  /**
   * Check if chunk data can be downloaded from external link based on download status
   */
  boolean isChunkDownloadable(ArrowResultChunk.DownloadStatus status) {
    return status == ArrowResultChunk.DownloadStatus.URL_FETCHED
        || status == ArrowResultChunk.DownloadStatus.CANCELLED
        || status == ArrowResultChunk.DownloadStatus.DOWNLOAD_FAILED_ABORTED;
  }

  /**
   * Release the memory for given chunk since it is already consumed
   * @param chunkIndex index of consumed chunk
   */
  public void releaseChunk(int chunkIndex) {
    if (chunkIndexToChunksMap.get(chunkIndex).releaseChunk()) {
      totalChunksInMemory--;
      synchronized (downloadMonitor) {
        downloadMonitor.notify();
      }
    }
  }

  /**
   * Initialize chunk with external link details
   * @param chunkIndex index of chunk
   * @param chunkLink external link details for chunk
   */
  void setChunkLink(long chunkIndex, ExternalLink chunkLink) {
    chunkIndexToChunksMap.get(chunkIndex).setChunkUrl(chunkLink);
  }

  /**
   * Fetches total chunks that we have in memory
   */
  long getTotalChunksInMemory() {
    return totalChunksInMemory;
  }

  /**
   * Release all chunks from memory. This would be called when result-set has been closed.
   */
  void releaseAllChunks() {
    this.isClosed = true;
    // TODO: release all chunks
  }

  @Override
  public void run() {
    // Starts the chunk download from given already downloaded position
    synchronized (downloadMonitor) {
      while (!this.isClosed && (nextChunkToDownload < totalChunks)) {
        try {
          while (totalChunksInMemory == allowedChunksInMemory
              || !isChunkDownloadable(chunkIndexToChunksMap.get(nextChunkToDownload).getStatus())) {
            downloadMonitor.wait();
          }
        } catch (InterruptedException e) {
          // Handle interruption
        }
        if (!this.isClosed) {
          ArrowResultChunk chunk = chunkIndexToChunksMap.get(nextChunkToDownload);
          this.chunkDownloaderExecutorService.submit(new SingleChunkDownloader(chunk, httpClient, session));
          totalChunksInMemory++;
          nextChunkToDownload++;
        }
      }
    }
  }
}
