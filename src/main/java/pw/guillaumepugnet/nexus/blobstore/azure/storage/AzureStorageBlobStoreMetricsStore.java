/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2017-present Sonatype, Inc.
 * All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
 * which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.
 *
 * Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
 * of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
 * Eclipse Foundation. All other trademarks are the property of their respective owners.
 */
package pw.guillaumepugnet.nexus.blobstore.azure.storage;

import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.sonatype.nexus.blobstore.AccumulatingBlobStoreMetrics;
import org.sonatype.nexus.blobstore.PeriodicJobService;
import org.sonatype.nexus.blobstore.PeriodicJobService.PeriodicJob;
import org.sonatype.nexus.blobstore.api.BlobStoreMetrics;

import org.sonatype.nexus.common.node.NodeAccess;
import org.sonatype.nexus.common.stateguard.Guarded;
import org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Long.parseLong;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.STARTED;

/**
 * A {@link BlobStoreMetricsStore} implementation that retains blobstore metrics in memory, periodically
 * writing them out to Azure Storage.
 */
@Named
public class AzureStorageBlobStoreMetricsStore
    extends StateGuardLifecycleSupport
{
  private static final String METRICS_SUFFIX = "-metrics";

  private static final String METRICS_EXTENSION = ".properties";

  private static final String TOTAL_SIZE_PROP_NAME = "totalSize";

  private static final String BLOB_COUNT_PROP_NAME = "blobCount";

  private static final int METRICS_FLUSH_PERIOD_SECONDS = 2;

  private final PeriodicJobService jobService;

  private AtomicLong blobCount;

  private final NodeAccess nodeAccess;

  private AtomicLong totalSize;

  private AtomicBoolean dirty;

  private PeriodicJob metricsWritingJob;

  private AzureStoragePropertiesFile propertiesFile;

  private final CloudBlobContainer container;

  public AzureStorageBlobStoreMetricsStore(final PeriodicJobService jobService,
                                            final NodeAccess nodeAccess,
                                           final CloudBlobContainer container) {
    this.jobService = checkNotNull(jobService);
    this.nodeAccess = checkNotNull(nodeAccess);
    this.container = checkNotNull(container);
  }

  @Override
  protected void doStart() throws Exception {
    blobCount = new AtomicLong();
    totalSize = new AtomicLong();
    dirty = new AtomicBoolean();

    propertiesFile = new AzureStoragePropertiesFile (container, nodeAccess.getId() + METRICS_SUFFIX + METRICS_EXTENSION);
    if (propertiesFile.exists()) {
      log.info("Loading blob store metrics file {}", propertiesFile);
      propertiesFile.load();
      readProperties();
    }
    else {
      log.info("Blob store metrics file {} not found - initializing at zero.", propertiesFile);
      updateProperties();
      propertiesFile.store();
    }

    jobService.startUsing();
    metricsWritingJob = jobService.schedule(() -> {
      try {
        if (dirty.compareAndSet(true, false)) {
          updateProperties();
          log.trace("Writing blob store metrics to {}", propertiesFile);
          propertiesFile.store();
        }
      }
      catch (Exception e) {
        // Don't propagate, as this stops subsequent executions
        log.error("Cannot write blob store metrics", e);
      }
    }, METRICS_FLUSH_PERIOD_SECONDS);
  }

  @Override
  protected void doStop() throws Exception {
    metricsWritingJob.cancel();
    metricsWritingJob = null;
    jobService.stopUsing();

    blobCount = null;
    totalSize = null;
    dirty = null;

    propertiesFile = null;
  }

  @Guarded(by = STARTED)
  public BlobStoreMetrics getMetrics() {
    Stream<AzureStoragePropertiesFile> blobStoreMetricsFiles = backingFiles();
    return getCombinedMetrics(blobStoreMetricsFiles);
  }

  private BlobStoreMetrics getCombinedMetrics(final Stream<AzureStoragePropertiesFile> blobStoreMetricsFiles) {
    AccumulatingBlobStoreMetrics blobStoreMetrics = new AccumulatingBlobStoreMetrics(0, 0, -1, true);

    blobStoreMetricsFiles.forEach(metricsFile -> {
        try {
          metricsFile.load();
          blobStoreMetrics.addBlobCount(parseLong(metricsFile.getProperty(BLOB_COUNT_PROP_NAME, "0")));
          blobStoreMetrics.addTotalSize(parseLong(metricsFile.getProperty(TOTAL_SIZE_PROP_NAME, "0")));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
    });
    return blobStoreMetrics;
  }

  @Guarded(by = STARTED)
  public void recordAddition(final long size) {
    blobCount.incrementAndGet();
    totalSize.addAndGet(size);
    dirty.set(true);
  }

  @Guarded(by = STARTED)
  public void recordDeletion(final long size) {
    blobCount.decrementAndGet();
    totalSize.addAndGet(-size);
    dirty.set(true);
  }

  public void remove() {
    backingFiles().forEach(metricsFile -> metricsFile.remove());
  }

  private Stream<AzureStoragePropertiesFile> backingFiles() {
     Stream<AzureStoragePropertiesFile> stream = StreamSupport.stream(container.listBlobs(nodeAccess.getId(),true).spliterator(), false)
              .filter(item -> ((CloudBlockBlob)item).getName().endsWith(METRICS_EXTENSION))
              .map(item -> new AzureStoragePropertiesFile(container,((CloudBlockBlob)item).getName()  ));

      return stream;
  }

  private void updateProperties() {
    propertiesFile.setProperty(TOTAL_SIZE_PROP_NAME, totalSize.toString());
    propertiesFile.setProperty(BLOB_COUNT_PROP_NAME, blobCount.toString());
  }

  private void readProperties() {
    String size = propertiesFile.getProperty(TOTAL_SIZE_PROP_NAME);
    if (size != null) {
      totalSize.set(parseLong(size));
    }

    String count = propertiesFile.getProperty(BLOB_COUNT_PROP_NAME);
    if (count != null) {
      blobCount.set(parseLong(count));
    }
  }
}
