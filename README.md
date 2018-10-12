<!--

    Sonatype Nexus (TM) Open Source Version
    Copyright (c) 2017-present Sonatype, Inc.
    All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.

    This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
    which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.

    Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
    of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
    Eclipse Foundation. All other trademarks are the property of their respective owners.

-->
Nexus Repository Azure Storage Blobstores
==============================

[![Join the chat at https://gitter.im/sonatype/nexus-developers](https://badges.gitter.im/sonatype/nexus-developers.svg)](https://gitter.im/sonatype/nexus-developers?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This project adds Azure Storage backed blobstores to Sonatype Nexus Repository 3.  It allows
Nexus Repository to store the components and assets in Azure Blob Storage instead of a
local filesystem.

Contribution Guidelines
-----------------------

Go read [our contribution guidelines](/.github/CONTRIBUTING.md) to get a bit more familiar with how
we would like things to flow.

Requirements
------------


* [Apache Maven 3.3.3+](https://maven.apache.org/install.html)
* [Java 8+](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* Network access to https://repository.sonatype.org/content/groups/sonatype-public-grid

Also, there is a good amount of information available at [Bundle Development Overview](https://help.sonatype.com/display/NXRM3/Bundle+Development#BundleDevelopment-BundleDevelopmentOverview)

Building
--------

To build the project and generate the bundle use Maven

    mvn clean install

If everything checks out, the nexus-blobstore-s3 bundle  should be available in the `target` folder


Installing
----------

See `install.sh`.  This copies the nexus-blobstore-azurestorage jar file to the
right place and updates the configuration files.  Use at your own
risk.

Alternatively, copy nexus-blobstore-azurestorage-*.jar and the Azure Storage SDK bundle
jar into the nexus/deploy subdirectory.

Start the bundle from the Nexus Repository console:

```
bundle:list | grep nexus-blobstore-azurestorage
bundle:start <bundleNumber>
```

Disclaimer
--------------

It is worth noting that this is **NOT SUPPORTED** by Sonatype, and is a contribution of ours
to the open source community (read: you!)

Remember:

* Use this contribution at the risk tolerance that you have
* Do NOT file Sonatype support tickets related to Azure support
* DO file issues here on GitHub, so that the community can pitch in