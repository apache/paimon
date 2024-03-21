The .mvn directory is needed to be able to use the ${maven.multiModuleProjectDirectory} property, since git cannot
commit an empty directory, add this file.

Once we do not use ${maven.multiModuleProjectDirectory}, we can remove this file and .mvn directory.