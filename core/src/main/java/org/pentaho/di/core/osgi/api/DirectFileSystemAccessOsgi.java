/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2023-2023 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.di.core.osgi.api;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.core.exception.KettleFileException;

/**
 * API to directly access a filesystem that will not in turn use the ConnectionManager. This exists to prevent infinite
 * recursion when the metastore implementation uses a filesystem, and the filesystem uses a ConnectionManager that in
 * turn uses the metastore.
 */
public interface DirectFileSystemAccessOsgi {

  /**
   * If the provided ConnectionDetails are the correct type, return a FileObject for the provided path. This method and
   * the returning FileObject must not depend on a ConnectionManager or MetaStore.
   *
   * @param connectionDetails for the connection
   * @param path path relative to the connection
   *
   * @return FileSystem or null if the provided connection details are not the matching type
   */
  FileObject getFile(ConnectionDetails connectionDetails, String path) throws KettleFileException;

}
