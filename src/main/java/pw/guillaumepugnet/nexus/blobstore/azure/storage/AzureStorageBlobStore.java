package pw.guillaumepugnet.nexus.blobstore.azure.storage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import java.util.concurrent.locks.Lock;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import com.sun.jndi.toolkit.url.Uri;
import org.sonatype.nexus.blobstore.*;
import org.sonatype.nexus.blobstore.api.Blob;
import org.sonatype.nexus.blobstore.api.BlobAttributes;
import org.sonatype.nexus.blobstore.api.BlobId;
import org.sonatype.nexus.blobstore.api.BlobMetrics;
import org.sonatype.nexus.blobstore.api.BlobStore;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;
import org.sonatype.nexus.blobstore.api.BlobStoreException;
import org.sonatype.nexus.blobstore.api.BlobStoreMetrics;
import org.sonatype.nexus.blobstore.api.BlobStoreUsageChecker;
import org.sonatype.nexus.common.node.NodeAccess;
import org.sonatype.nexus.common.stateguard.Guarded;
import org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.HashCode;
import org.joda.time.DateTime;


import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.CacheLoader.from;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.cache.CacheLoader.from;
import static org.sonatype.nexus.blobstore.DirectPathLocationStrategy.DIRECT_PATH_ROOT;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.*;


@Named(AzureStorageBlobStore.TYPE)
public class AzureStorageBlobStore
    extends StateGuardLifecycleSupport
    implements BlobStore
{

    public static final String TYPE = "Azure Storage";

    public static final String CONFIG_KEY = "azurestorage";

    public static final String CONFIG_CONNECTION_STRING_KEY = "connectionstring";

    public static final String CONFIG_CONTAINER_NAME_KEY = "nexuscontainer";

    public static final String BLOB_CONTENT_SUFFIX = ".bytes";

    public static final String BLOB_ATTRIBUTE_SUFFIX = ".properties";

    public static final String TEMPORARY_BLOB_ID_PREFIX = "tmp$";

    public static final String CONTENT_PREFIX = "content";

    public static final String METADATA_FILENAME = "metadata.properties";

    public static final String DIRECT_PATH_PREFIX = CONTENT_PREFIX + "/" + DIRECT_PATH_ROOT;

    public static final String TYPE_KEY = "type";

    public static final String TYPE_V1 = "AzureStorage/1";

    private final BlobIdLocationResolver blobIdLocationResolver;
    private BlobStoreConfiguration blobStoreConfiguration;
   // private S3BlobStoreMetricsStore storeMetrics;
    private LoadingCache<BlobId, AzureStorageBlob> liveBlobs;
    private CloudBlobClient client;
    private CloudBlobContainer container;
    private AzureStorageBlobStoreMetricsStore storeMetrics;
    private final NodeAccess nodeAccess;
    private final PeriodicJobService jobService;
    @Inject
    public AzureStorageBlobStore(final BlobIdLocationResolver blobIdLocationResolver,
                                 final PeriodicJobService jobService,
                                 final NodeAccess nodeAccess)
    {
        this.blobIdLocationResolver = checkNotNull(blobIdLocationResolver);
        this.nodeAccess = checkNotNull(nodeAccess);
        this.jobService = checkNotNull(jobService);
    }

    @Override
    protected void doStart() throws Exception {
        // ensure blobstore is supported
        AzureStoragePropertiesFile metadata = new AzureStoragePropertiesFile(container, METADATA_FILENAME);
        if (metadata.exists())
        {
            metadata.load();
            String type = metadata.getProperty(TYPE_KEY);
            checkState(TYPE_V1.equals(type), "Unsupported blob store type/version: %s in %s", type, metadata);
        }
        else
            {
            // assumes new blobstore, write out type
            metadata.setProperty(TYPE_KEY, TYPE_V1);
            metadata.store();
        }

        liveBlobs = CacheBuilder.newBuilder().weakValues().build(from(AzureStorageBlob::new));

        storeMetrics = new AzureStorageBlobStoreMetricsStore(jobService, nodeAccess, container);
        storeMetrics.start();
    }

    @Override
    protected void doStop() throws Exception {
        liveBlobs = null;
        storeMetrics.stop();
    }

    @Override
    @Guarded(by = STARTED)
    public Blob create(final InputStream blobData, final Map<String, String> headers) {
        checkNotNull(blobData);
        return create(headers, destination -> {
            try (InputStream data = blobData) {
                MetricsInputStream input = new MetricsInputStream(data);
                CloudBlockBlob blockBlob = container.getBlockBlobReference(destination);
                blockBlob.upload(blobData,input.getSize());
                return input.getMetrics();
            } catch (StorageException e) {
                throw new BlobStoreException("error uploading blob", e, null);
            } catch (URISyntaxException e) {
                throw new BlobStoreException("error uploading blob", e, null);
            }
        });
    }

    @Override
    @Guarded(by = STARTED)
    public Blob create(final Path sourceFile, final Map<String, String> headers, final long size, final HashCode sha1) {
        throw new BlobStoreException("hard links not supported", null);
    }

    private Blob create(final Map<String, String> headers, final BlobIngester ingester) {
        checkNotNull(headers);

        checkArgument(headers.containsKey(BLOB_NAME_HEADER), "Missing header: %s", BLOB_NAME_HEADER);
        checkArgument(headers.containsKey(CREATED_BY_HEADER), "Missing header: %s", CREATED_BY_HEADER);

        final BlobId blobId = blobIdLocationResolver.fromHeaders(headers);

        //    return CONTENT_PREFIX + "/" + blobIdLocationResolver.getLocation(id) + BLOB_CONTENT_SUFFIX;
        final String blobPath = contentPath(blobId);
        final String attributePath = attributePath(blobId);
        final AzureStorageBlob blob = liveBlobs.getUnchecked(blobId);

        Lock lock = blob.lock();
        try
        {
            log.debug("Writing blob {} to {}", blobId, blobPath);
            final StreamMetrics streamMetrics = ingester.ingestTo(blobPath);
            final BlobMetrics metrics = new BlobMetrics(new DateTime(), streamMetrics.getSha1(), streamMetrics.getSize());
            AzureStorageBlobAttributes blobAttributes = new AzureStorageBlobAttributes(container, attributePath, headers, metrics);
            blobAttributes.store();
            storeMetrics.recordAddition(blobAttributes.getMetrics().getContentSize());
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally {
            lock.unlock();
        }
        return blob;
    }


    @Override
    public Blob copy(BlobId blobId, Map<String, String> headers) {
        Blob sourceBlob = checkNotNull(get(blobId));
        String sourcePath = contentPath(sourceBlob.getId());

        return create(headers, destination -> {
            CloudBlockBlob sourceBlockBlob;
            try {
                sourceBlockBlob = container.getBlockBlobReference(sourcePath);
                CloudBlockBlob destinationBlockBlob = container.getBlockBlobReference(destination);
                destinationBlockBlob.startCopy(sourceBlockBlob);
                waitForCopyToComplete(destinationBlockBlob);

            } catch (URISyntaxException e) {
                throw new BlobStoreException("error copying blob", e, null);
            } catch (StorageException e) {
                throw new BlobStoreException("error copying blob", e, null);
            } catch (InterruptedException e) {
                throw new BlobStoreException("error copying blob", e, null);
            }


            BlobMetrics metrics = sourceBlob.getMetrics();
            return new StreamMetrics(metrics.getContentSize(), metrics.getSha1Hash());
        });
    }

    @Nullable
    @Override
    @Guarded(by = STARTED)
    public Blob get(final BlobId blobId) {
        return get(blobId, false);
    }

    @Nullable
    @Override
    public Blob get(BlobId blobId, boolean includeDeleted) {
        checkNotNull(blobId);

        AzureStorageBlob blob = liveBlobs.getUnchecked(blobId);

        if (blob.isStale())
        {
            Lock lock = blob.lock();
            try {
                if (blob.isStale()) {
                 AzureStorageBlobAttributes   blobAttributes = new AzureStorageBlobAttributes(container, attributePath(blobId).toString());
                    boolean loaded = blobAttributes.load();
                    if (!loaded) {
                        log.warn("Attempt to access non-existent blob {} ({})", blobId, blobAttributes);
                        return null;
                    }

                    if (blobAttributes.isDeleted() && !includeDeleted) {
                        log.warn("Attempt to access soft-deleted blob {} ({})", blobId, blobAttributes);
                        return null;
                    }

                    blob.refresh(blobAttributes.getHeaders(), blobAttributes.getMetrics());
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }

        log.debug("Accessing blob {}", blobId);

        return blob;
    }

    @Override
    public boolean delete(BlobId blobId, String reason) {
        checkNotNull(blobId);

        final AzureStorageBlob blob = liveBlobs.getUnchecked(blobId);

        Lock lock = blob.lock();
        try {
            log.debug("Soft deleting blob {}", blobId);

            AzureStorageBlobAttributes blobAttributes = new AzureStorageBlobAttributes(container, attributePath(blobId).toString());

            boolean loaded = blobAttributes.load();
            if (!loaded) {
                // This could happen under some concurrent situations (two threads try to delete the same blob)
                // but it can also occur if the deleted index refers to a manually-deleted blob.
                log.warn("Attempt to mark-for-delete non-existent blob {}", blobId);
                return false;
            }
            else if (blobAttributes.isDeleted()) {
                log.debug("Attempt to delete already-deleted blob {}", blobId);
                return false;
            }

            blobAttributes.setDeleted(true);
            blobAttributes.setDeletedReason(reason);
            blobAttributes.store();

            delete(contentPath(blobId));

            blob.markStale();

            return true;
        }
        catch (Exception e) {
            throw new BlobStoreException(e, blobId);
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public boolean deleteHard(BlobId blobId) {
        checkNotNull(blobId);

        try {
            log.debug("Hard deleting blob {}", blobId);

            String attributePath = attributePath(blobId);
            AzureStorageBlobAttributes blobAttributes = new AzureStorageBlobAttributes(container, attributePath);
            Long contentSize = getContentSizeForDeletion(blobAttributes);

            String blobPath = contentPath(blobId);

            boolean blobDeleted = delete(blobPath);
            delete(attributePath);

            if (blobDeleted && contentSize != null) {
                storeMetrics.recordDeletion(contentSize);
            }

            return blobDeleted;
        }
        finally {
            liveBlobs.invalidate(blobId);
        }
    }

        private boolean delete(final String path) {
        boolean deleted;
        try {
            CloudBlockBlob blockBlob = container.getBlockBlobReference(path);
           deleted =  blockBlob.deleteIfExists();
        } catch (URISyntaxException e) {
            throw new BlobStoreException("error deleting blob: " + path, e, null);
        } catch (StorageException e) {
            throw new BlobStoreException("error deleting blob: " + path, e, null);
        }
        return deleted;
    }

    @Nullable
    private Long getContentSizeForDeletion(final AzureStorageBlobAttributes blobAttributes) {
        try {
            blobAttributes.load();
            return blobAttributes.getMetrics() != null ? blobAttributes.getMetrics().getContentSize() : null;
        }
        catch (Exception e) {
            log.warn("Unable to load attributes {}, delete will not be added to metrics.", blobAttributes, e);
            return null;
        }
    }

    @Override
    @Guarded(by = STARTED)
    public BlobStoreMetrics getMetrics() {
        return storeMetrics.getMetrics();
    }

    @Override
    @Guarded(by = STARTED)
    public synchronized void compact() {
        compact(null);
    }

    @Override
    @Guarded(by = STARTED)
    public synchronized void compact(@Nullable final BlobStoreUsageChecker inUseChecker) {
        // no-op
    }

    @Override
    public BlobStoreConfiguration getBlobStoreConfiguration() {
        return this.blobStoreConfiguration;
    }

    @Override
    public void init(final BlobStoreConfiguration configuration) {
        this.blobStoreConfiguration = configuration;

        String connectionString = blobStoreConfiguration.attributes(CONFIG_KEY).get(CONFIG_CONNECTION_STRING_KEY, String.class);
        if(!Strings.isNullOrEmpty(connectionString)) {
            throw new BlobStoreException("Unable to retrieve connection string from BlobStoreConfiguration", null, null);
        }

        CloudStorageAccount storageAccount;
        try {
            storageAccount = CloudStorageAccount.parse(connectionString);
        } catch (URISyntaxException e) {
            throw new BlobStoreException("Unable to parse CloudStorageAccount connection string", e, null);
        } catch (InvalidKeyException e) {
            throw new BlobStoreException("Unable to parse CloudStorageAccount connection string", e, null);
        }

        String containerName = blobStoreConfiguration.attributes(CONFIG_KEY).get(CONFIG_CONTAINER_NAME_KEY, String.class);
        if(!Strings.isNullOrEmpty(containerName)) {
            throw new BlobStoreException("Unable to retrieve container name from BlobStoreConfiguration", null, null);
        }

        try {
        this.client = storageAccount.createCloudBlobClient();
        this.container = client.getContainerReference(containerName);
        this.container.createIfNotExists(BlobContainerPublicAccessType.OFF, // prevent anonymous access
                new BlobRequestOptions(),
                new OperationContext());
        }
        catch (Exception e) {
            throw new BlobStoreException("Unable to initialize blob store container: " + containerName, e, null);
        }
    }

    /**
     * Delete files known to be part of the AzureStorageBlobStore implementation if the content directory is empty.
     */
    @Override
    @Guarded(by = {NEW, STOPPED, FAILED})
    public void remove() {
            Iterable<ListBlobItem> items =  container.listBlobs(CONTENT_PREFIX + "/");
           if (Iterables.size(items) == 0)  {
                AzureStoragePropertiesFile metadata = new AzureStoragePropertiesFile(container, METADATA_FILENAME);
                metadata.remove();
                storeMetrics.remove();

               try
               {
                   container.deleteIfExists();
               }
               catch (StorageException e)
               {
                   throw new BlobStoreException("Unable to delete container: " + CONFIG_CONTAINER_NAME_KEY , e, null);
               }
           }
            else {
                log.warn("Unable to delete non-empty container {}",  CONFIG_CONTAINER_NAME_KEY );
            }

    }

    private interface BlobIngester {
        StreamMetrics ingestTo(final String destination) throws IOException;
    }

    @Override
    public Stream<BlobId> getBlobIdStream() {
        return StreamSupport.stream(container.listBlobs().spliterator(), false)
                .map(ListBlobItem::getUri)
                .map(java.net.URI::getPath)
                .map(path -> path.substring(path.lastIndexOf('/') + 1,path.length()))
                .filter(filename -> filename.endsWith(BLOB_ATTRIBUTE_SUFFIX) && !filename.startsWith(TEMPORARY_BLOB_ID_PREFIX))
                .map(filename -> filename.substring(0, filename.length() - BLOB_ATTRIBUTE_SUFFIX.length()))
                .map(BlobId::new);
    }

    @Override
    public Stream<BlobId> getDirectPathBlobIdStream(final String prefix) {
        String subpath = format("%s/%s", DIRECT_PATH_PREFIX, prefix);

        return stream(container.listBlobs(subpath).spliterator(), false)
                .map(ListBlobItem::getUri)
                .map(java.net.URI::getPath)
                .filter(path -> path.endsWith(BLOB_ATTRIBUTE_SUFFIX))
                .map(this::attributePathToDirectPathBlobId);
    }

    @Override
    public BlobAttributes getBlobAttributes(final BlobId blobId) {
        try {
            AzureStorageBlobAttributes blobAttributes = new AzureStorageBlobAttributes(container, attributePath(blobId));
            return blobAttributes.load() ? blobAttributes : null;
        }
        catch (IOException e) {
            log.error("Unable to load S3BlobAttributes for blob id: {}", blobId, e);
            return null;
        }
    }

    @Override
    public void setBlobAttributes(BlobId blobId, BlobAttributes blobAttributes) {
        try {
           // S3BlobAttributes s3BlobAttributes = (S3BlobAttributes) getBlobAttributes(blobId);
           // s3BlobAttributes.updateFrom(blobAttributes);
           // s3BlobAttributes.store();
        }
        catch (Exception e) {
            log.error("Unable to set BlobAttributes for blob id: {}, exception: {}",
                    blobId, e.getMessage(), log.isDebugEnabled() ? e : null);
        }
    }

    /**
     * Returns path for blob-id content file relative to root directory.
     */
    private String contentPath(final BlobId id) {
        return getLocation(id) + BLOB_CONTENT_SUFFIX;
    }

    /**
     * Returns path for blob-id attribute file relative to root directory.
     */
    private String attributePath(final BlobId id) {
        return getLocation(id) + BLOB_ATTRIBUTE_SUFFIX;
    }

    /**
     * Returns the location for a blob ID based on whether or not the blob ID is for a temporary or permanent blob.
     */
    private String getLocation(final BlobId id) {
           return CONTENT_PREFIX + "/" + blobIdLocationResolver.getLocation(id);
    }

    class AzureStorageBlob
            extends BlobSupport
    {
        AzureStorageBlob(final BlobId blobId) {
            super(blobId);
        }

        @Override
        public InputStream getInputStream() {
            String blobName = contentPath(getId());
            InputStream inputStream = null;
            CloudBlockBlob blockBlobReference = null;

            try {
                blockBlobReference = container.getBlockBlobReference(blobName);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            } catch (StorageException e) {
                e.printStackTrace();
            }
            try {
                inputStream = blockBlobReference.openInputStream();
            } catch (StorageException e) {
                e.printStackTrace();
            }
            return inputStream;
        }
    }

    /**
     * Wait until the copy complete.
     *
     * @param blob Target of the copy operation
     *
     * @throws InterruptedException
     * @throws StorageException
     */
    private static void waitForCopyToComplete(CloudBlob blob) throws InterruptedException, StorageException {
        CopyStatus copyStatus = CopyStatus.PENDING;
        while (copyStatus == CopyStatus.PENDING) {
            Thread.sleep(1000);
            copyStatus = blob.getCopyState().getStatus();
        }
    }

    /**
     * Used by {@link #getDirectPathBlobIdStream(String)} to convert an azure storage key to a {@link BlobId}.
     *
     * @see BlobIdLocationResolver
     */
    private BlobId attributePathToDirectPathBlobId(final String key) {
        checkArgument(key.startsWith(DIRECT_PATH_PREFIX + "/"), "Not direct path blob path: %s", key);
        checkArgument(key.endsWith(BLOB_ATTRIBUTE_SUFFIX), "Not blob attribute path: %s", key);
        String blobName = key
                .substring(0, key.length() - BLOB_ATTRIBUTE_SUFFIX.length())
                .substring(DIRECT_PATH_PREFIX.length() + 1);
        Map<String, String> headers = ImmutableMap.of(
                BLOB_NAME_HEADER, blobName,
                DIRECT_PATH_BLOB_HEADER, "true"
        );
        return blobIdLocationResolver.fromHeaders(headers);
    }
}
