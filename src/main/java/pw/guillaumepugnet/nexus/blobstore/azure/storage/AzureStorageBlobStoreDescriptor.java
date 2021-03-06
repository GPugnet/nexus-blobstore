/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2008-present Sonatype, Inc.
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

import org.sonatype.goodies.i18n.I18N;
import org.sonatype.goodies.i18n.MessageBundle;
import org.sonatype.nexus.blobstore.BlobStoreDescriptor;
import org.sonatype.nexus.formfields.FormField;
import org.sonatype.nexus.formfields.NumberTextFormField;
import org.sonatype.nexus.formfields.PasswordFormField;
import org.sonatype.nexus.formfields.StringTextFormField;

import javax.inject.Named;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link BlobStoreDescriptor} for {@link AzureStorageBlobStore}.
 *
 * @since 3.4
 */
@Named(AzureStorageBlobStoreDescriptor.TYPE)
public class AzureStorageBlobStoreDescriptor
    implements BlobStoreDescriptor
{
  public static final String TYPE = "AzureStorage";

  private interface Messages
      extends MessageBundle
  {
    @DefaultMessage("Azure Storage container name")
    String containerNameLabel();

    @DefaultMessage("The name of the Azure Storage container where the blobs will be stored.")
    String containerNameHelp();

    @DefaultMessage("Azure Storage connection string")
    String connectionStringLabel();

    @DefaultMessage("The connection string to connect to the Azure Storage.")
    String connectionStringHelp();

  }

  private static final Messages messages = I18N.create(Messages.class);

  private final FormField containerName;
  private final FormField connectionString;

  public AzureStorageBlobStoreDescriptor() {
    this.containerName = new StringTextFormField(
            AzureStorageBlobStore.CONFIG_CONTAINER_NAME_KEY,
            messages.containerNameLabel(),
            messages.containerNameHelp(),
            FormField.MANDATORY
    );

    this.connectionString = new StringTextFormField(
            AzureStorageBlobStore.CONFIG_CONNECTION_STRING_KEY,
            messages.connectionStringLabel(),
            messages.connectionStringHelp(),
            FormField.MANDATORY
    );
  }

  @Override
  public String getName() {
    return AzureStorageBlobStore.TYPE;
  }

  @Override
  public List<FormField> getFormFields() {
      return Arrays.asList(containerName,connectionString);
  }
}
