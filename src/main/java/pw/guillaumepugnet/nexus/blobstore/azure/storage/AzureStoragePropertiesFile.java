package pw.guillaumepugnet.nexus.blobstore.azure.storage;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.nexus.blobstore.api.BlobStoreException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Persistent properties file stored in Azure Storage.
 */
public class AzureStoragePropertiesFile
    extends Properties
{
  private static final Logger log = LoggerFactory.getLogger(AzureStoragePropertiesFile.class);

  private final CloudBlobContainer container;

  private final String key;

  public AzureStoragePropertiesFile(final CloudBlobContainer container, final String key) {
    this.container = checkNotNull(container);
    this.key = checkNotNull(key);
  }

  public void load() throws IOException {
      log.debug("Loading: {}", key);
      try {
          CloudBlockBlob blobReference =  container.getBlockBlobReference(key);
          load(blobReference.openInputStream());
      } catch (URISyntaxException e) {
          throw new BlobStoreException("error loading: " + key, e, null);
      } catch (StorageException e) {
          throw new BlobStoreException("error loading: " + key, e, null);
      }
  }

  public void store() throws IOException {
    log.debug("Storing: {}", key);

    ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
    store(bufferStream, null);
    byte[] buffer = bufferStream.toByteArray();

      try {
          CloudBlockBlob blockBlob = container.getBlockBlobReference(key);
          blockBlob.upload(new ByteArrayInputStream(buffer),buffer.length);

      } catch (URISyntaxException e) {
          throw new BlobStoreException("error loading: " + key, e, null);
      } catch (StorageException e) {
          throw new BlobStoreException("error loading: " + key, e, null);
      }
  }

  public boolean exists()
  {
    boolean exists;
    try
    {
        exists =          container.getBlockBlobReference(key).exists();
        }
        catch (URISyntaxException e) {
          throw new BlobStoreException("error checking if blob exists: " + key, e, null);
      } catch (StorageException e) {
          throw new BlobStoreException("error checking if blob exists: " + key, e, null);
      }
      return exists;
  }

  public void remove() {
      try
      {
        CloudBlockBlob blockBlob =          container.getBlockBlobReference(key);
        blockBlob.deleteIfExists();
      }
      catch (URISyntaxException e) {
          throw new BlobStoreException("error deleting blob : " + key, e, null);
      } catch (StorageException e) {
          throw new BlobStoreException("error deleting blob : " + key, e, null);
      }
  }

  public String toString() {
    return getClass().getSimpleName() + "{" +
        "key=" + key +
        '}';
  }
}
