/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2002-2020 Hitachi Vantara..  All rights reserved.
 */

package org.pentaho.di.metastore;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.metastore.api.BaseMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreDependenciesExistsException;
import org.pentaho.metastore.api.exceptions.MetaStoreElementExistException;
import org.pentaho.metastore.api.exceptions.MetaStoreElementTypeExistsException;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.api.exceptions.MetaStoreNamespaceExistsException;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreAttribute;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.security.IMetaStoreElementOwner;
import org.pentaho.metastore.api.security.MetaStoreElementOwnerType;
import org.pentaho.metastore.stores.xml.AutomaticXmlMetaStoreCache;
import org.pentaho.metastore.stores.xml.XmlMetaStoreAttribute;
import org.pentaho.metastore.stores.xml.XmlMetaStoreCache;
import org.pentaho.metastore.stores.xml.XmlMetaStoreElementOwner;
import org.pentaho.metastore.stores.xml.XmlUtil;

public class XmlVfsMetaStore extends BaseMetaStore implements IMetaStore {

  private static final String VFS_PREFIX = "file://";
  // example vfs URL: pvfs://s3test/testbucket/
  // "s3test" is the name of the connection
  private static final String VFS_PREFIX_CONFIG = "METASTORE_VFS_PREFIX";

  // This is *relative to* rootFile
  private String rootFolder;
  private FileObject rootFile;

  // TODO: fix paths in cache
  private final XmlMetaStoreCache metaStoreCache;

  public XmlVfsMetaStore( FileObject rootFile ) throws MetaStoreException {
    this( rootFile, new AutomaticXmlMetaStoreCache() );
  }

  public XmlVfsMetaStore( FileObject rootFile, XmlMetaStoreCache metaStoreCacheImpl ) throws MetaStoreException {
    this.rootFolder = XmlUtil.META_FOLDER_NAME;
    this.rootFile = rootFile;
    try {
      FileObject metaFolder = rootFile.resolveFile( rootFolder );
      if ( !metaFolder.exists() ) {
        metaFolder.createFolder();
      }
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( "Unable to create XML VFS meta store root folder: " + this.rootFolder, ex );
    }

    // Give the MetaStore a default name
    //
    setName( "VFS Metastore: " + rootFile.getName().getFriendlyURI() );
    metaStoreCache = metaStoreCacheImpl;
  }

  @Override
  public boolean equals( Object obj ) {
    if ( this == obj ) {
      return true;
    }
    if ( !( obj instanceof XmlVfsMetaStore ) ) {
      return false;
    }
    return ((XmlVfsMetaStore) obj ).name.equalsIgnoreCase( name );
  }

  @Override
  public synchronized List<String> getNamespaces() throws MetaStoreException {
    lockStore();
    try {
      FileObject metaFolder = rootFile.resolveFile( rootFolder );
      List<FileObject> files = listFolders( metaFolder );
      List<String> namespaces = new ArrayList<>( files.size() );
      for ( FileObject file : files ) {
        namespaces.add( file.getName().getBaseName() );
      }
      return namespaces;
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( "Unable to read XML VFS meta store root folder: " + this.rootFolder, ex );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized boolean namespaceExists( String namespace ) throws MetaStoreException {
    lockStore();
    try {
      String spaceFolder = XmlUtil.getNamespaceFolder( rootFolder, namespace );
      FileObject spaceFile = rootFile.resolveFile( spaceFolder );
      return spaceFile.exists();
    } catch ( FileSystemException  ex ) {
      throw new MetaStoreException( ex );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized void createNamespace( String namespace ) throws MetaStoreException {
    lockStore();
    try {
      String spaceFolder = XmlUtil.getNamespaceFolder( rootFolder, namespace );
      FileObject spaceFile = rootFile.resolveFile( spaceFolder );
      if ( spaceFile.exists() ) {
        throw new MetaStoreNamespaceExistsException( "The namespace with name '" + namespace + "' already exists." );
      }
      spaceFile.createFolder();
    } catch ( FileSystemException  ex ) {
      throw new MetaStoreException( ex );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized void deleteNamespace( String namespace ) throws MetaStoreException {
    lockStore();
    try {
      String spaceFolder = XmlUtil.getNamespaceFolder( rootFolder, namespace );
      FileObject spaceFile = rootFile.resolveFile( spaceFolder );
      if ( !spaceFile.exists() ) {
        return; // Should we throw an exception?
      }
      List<IMetaStoreElementType> elementTypes = getElementTypes( namespace, false );

      if ( !elementTypes.isEmpty() ) {
        List<String> dependencies = new ArrayList<>( elementTypes.size() );
        for ( IMetaStoreElementType elementType : elementTypes ) {
          dependencies.add( elementType.getId() );
        }
        throw new MetaStoreDependenciesExistsException( dependencies,
                                                        "Unable to delete the XML meta store namespace with name '" + namespace
                                                        + "' as it still contains dependencies" );
      }

      if ( !spaceFile.delete() ) {
        throw new MetaStoreException( "Unable to delete XML meta store namespace folder, check to see if it's empty" );
      }
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( ex );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized List<IMetaStoreElementType> getElementTypes( String namespace ) throws MetaStoreException {
    return getElementTypes( namespace, true );
  }

  protected synchronized List<IMetaStoreElementType> getElementTypes( String namespace, boolean lock )
    throws MetaStoreException {
    if ( lock ) {
      lockStore();
    }
    try {
      String spaceFolder = XmlUtil.getNamespaceFolder( rootFolder, namespace );
      FileObject spaceFolderFile = rootFile.resolveFile( spaceFolder );
      List<FileObject> elementTypeFolders = listFolders( spaceFolderFile );
      List<IMetaStoreElementType> elementTypes = new ArrayList<>( elementTypeFolders.size() );
      for ( FileObject elementTypeFolder : elementTypeFolders ) {
        String elementTypeId = elementTypeFolder.getName().getBaseName();
        IMetaStoreElementType elementType = getElementType( namespace, elementTypeId, false );
        if ( elementType != null ) {
          elementTypes.add( elementType );
        }
      }
      return elementTypes;
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( ex );
    } finally {
      if ( lock ) {
        unlockStore();
      }
    }
  }

  @Override
  public synchronized List<String> getElementTypeIds( String namespace ) throws MetaStoreException {
    lockStore();
    try {
      String spaceFolder = XmlUtil.getNamespaceFolder( rootFolder, namespace );
      FileObject spaceFolderFile = rootFile.resolveFile( spaceFolder );
      List<FileObject> elementTypeFolders = listFolders( spaceFolderFile );
      List<String> ids = new ArrayList<>( elementTypeFolders.size() );
      for ( FileObject elementTypeFolder : elementTypeFolders ) {
        String elementTypeId = elementTypeFolder.getName().getBaseName();
        ids.add( elementTypeId );
      }

      return ids;
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( ex );
    } finally {
      unlockStore();
    }
  }

  protected synchronized XmlVfsMetaStoreElementType getElementType( String namespace, String elementTypeId, boolean lock )
    throws MetaStoreException {
    if ( lock ) {
      lockStore();
    }
    try {
      String elementTypeFile = XmlUtil.getElementTypeFile( rootFolder, namespace, elementTypeId );

      FileObject spaceFolderFile = rootFile.resolveFile( elementTypeFile );

      if ( spaceFolderFile.exists() ) {
        XmlVfsMetaStoreElementType elementType = new XmlVfsMetaStoreElementType( namespace, elementTypeFile, spaceFolderFile.getContent().getInputStream() );
        elementType.setMetaStoreName( getName() );
        return elementType;
      } else {
        return null;
      }
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( ex );
    } finally {
      if ( lock ) {
        unlockStore();
      }
    }
  }

  public synchronized XmlVfsMetaStoreElementType getElementType( String namespace, String elementTypeId )
    throws MetaStoreException {
    return getElementType( namespace, elementTypeId, true );
  }

  @Override
  public synchronized XmlVfsMetaStoreElementType getElementTypeByName( String namespace, String elementTypeName )
    throws MetaStoreException {
    for ( IMetaStoreElementType elementType : getElementTypes( namespace ) ) {
      if ( elementType.getName() != null && elementType.getName().equalsIgnoreCase( elementTypeName ) ) {
        return (XmlVfsMetaStoreElementType) elementType;
      }
    }
    return null;
  }

  @Override
  public synchronized XmlVfsMetaStoreElementType getElementTypeByName( String namespace, String elementTypeName, boolean lock )
    throws MetaStoreException {
    for ( IMetaStoreElementType elementType : getElementTypes( namespace, lock ) ) {
      if ( elementType.getName() != null && elementType.getName().equalsIgnoreCase( elementTypeName ) ) {
        return (XmlVfsMetaStoreElementType) elementType;
      }
    }
    return null;
  }

  public IMetaStoreAttribute newAttribute( String id, Object value ) throws MetaStoreException {
    return new XmlMetaStoreAttribute( id, value );
  }

  @Override
  public synchronized void createElementType( String namespace, IMetaStoreElementType elementType )
    throws MetaStoreException {
    lockStore();
    try {
      // In the case of a file, the ID is the name
      //
      if ( elementType.getId() == null ) {
        elementType.setId( elementType.getName() );
      }

      String elementTypeFolder = XmlUtil.getElementTypeFolder( rootFolder, namespace, elementType.getName() );
      FileObject elementTypeFolderFile = rootFile.resolveFile( elementTypeFolder );

      String elementTypeFilename = XmlUtil.getElementTypeFile( rootFolder, namespace, elementType.getName() );
      FileObject elementTypeFile = rootFile.resolveFile( elementTypeFilename );
      if ( elementTypeFolderFile.exists() && elementTypeFile.exists() ) {
        throw new MetaStoreElementTypeExistsException( getElementTypes( namespace, false ),
                                                       "The specified element type already exists with the same ID" );
      }
      if ( !elementTypeFolderFile.exists() ) {
        elementTypeFolderFile.createFolder();
      }


      // Copy the element type information to the XML meta store
      //
      XmlVfsMetaStoreElementType xmlType =
        new XmlVfsMetaStoreElementType( namespace, elementType.getId(), elementType.getName(), elementType
                                        .getDescription() );
      xmlType.setFilename( elementTypeFilename );
      xmlType.setFileObject( elementTypeFile );
      xmlType.save();

      metaStoreCache.registerElementTypeIdForName( namespace, elementType.getName(), elementType.getId() );
      metaStoreCache.registerProcessedFile( elementTypeFolder, elementTypeFolderFile.getContent().getLastModifiedTime() );

      xmlType.setMetaStoreName( getName() );
      elementType.setMetaStoreName( getName() );
    } catch ( FileSystemException  ex ) {
      throw new MetaStoreException( ex );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized void updateElementType( String namespace, IMetaStoreElementType elementType )
    throws MetaStoreException {
    lockStore();
    try {
      String elementTypeFolder = XmlUtil.getElementTypeFolder( rootFolder, namespace, elementType.getName() );
      FileObject elementTypeFolderFile = rootFile.resolveFile( elementTypeFolder );

      if ( !elementTypeFolderFile.exists() ) {
        throw new MetaStoreException( "The specified element type with ID '" + elementType.getId()
                                      + "' doesn't exists so we can't update it." );
      }

      String elementTypeFilename = XmlUtil.getElementTypeFile( rootFolder, namespace, elementType.getName() );
      FileObject elementTypeFile = rootFile.resolveFile( elementTypeFilename );

      // Save the element type information to the XML meta store
      //
      XmlVfsMetaStoreElementType xmlType =
        new XmlVfsMetaStoreElementType( namespace, elementType.getId(), elementType.getName(), elementType
                                        .getDescription() );
      xmlType.setFilename( elementTypeFilename );
      xmlType.setFileObject( elementTypeFile );
      xmlType.save();

      metaStoreCache.registerElementTypeIdForName( namespace, elementType.getName(), elementType.getId() );
      metaStoreCache.registerProcessedFile( elementTypeFolder, elementTypeFolderFile.getContent().getLastModifiedTime() );
    } catch ( FileSystemException  ex ) {
      throw new MetaStoreException( ex );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized void deleteElementType( String namespace, IMetaStoreElementType elementType )
    throws MetaStoreException {
    lockStore();
    try {
      String elementTypeFilename = XmlUtil.getElementTypeFile( rootFolder, namespace, elementType.getName() );
      FileObject elementTypeFile = rootFile.resolveFile( elementTypeFilename );

      if ( !elementTypeFile.exists() ) {
        return;
      }
      // Check if the element type has no remaining elements
      List<IMetaStoreElement> elements = getElements( namespace, elementType, false, true, new ArrayList<MetaStoreException>() );
      if ( !elements.isEmpty() ) {
        List<String> dependencies = new ArrayList<>();
        for ( IMetaStoreElement element : elements ) {
          dependencies.add( element.getId() );
        }
        throw new MetaStoreDependenciesExistsException( dependencies, "Unable to delete element type with name '"
                                                        + elementType.getName() + "' in namespace '" + namespace + "' because there are still elements present" );
      }

      // Remove the elementType.xml file
      //
      if ( !elementTypeFile.delete() ) {
        throw new MetaStoreException( "Unable to delete element type XML file '" + elementTypeFilename + "'" );
      }

      // Remove the folder too, should be empty by now.
      //
      String elementTypeFolder = XmlUtil.getElementTypeFolder( rootFolder, namespace, elementType.getName() );
      FileObject elementTypeFolderFile = rootFile.resolveFile( elementTypeFolder );
      if ( !elementTypeFolderFile.delete() ) {
        throw new MetaStoreException( "Unable to delete element type XML folder '" + elementTypeFolder + "'" );
      }
      metaStoreCache.unregisterElementTypeId( namespace, elementType.getId() );
      metaStoreCache.unregisterProcessedFile( elementTypeFolder );
    } catch ( FileSystemException  ex ) {
      throw new MetaStoreException( ex );
    } finally {
      unlockStore();
    }
  }

  @Override
  public List<IMetaStoreElement> getElements( String namespace, IMetaStoreElementType elementType )
    throws MetaStoreException {
    return getElements( namespace, elementType, true, true, null );
  }

  @Override
  public List<IMetaStoreElement> getElements( String namespace, IMetaStoreElementType elementType, boolean lock )
    throws MetaStoreException {
    return getElements( namespace, elementType, lock, true, null );
  }

  @Override
  public List<IMetaStoreElement> getElements( String namespace, IMetaStoreElementType elementType, boolean lock,
                                              List<MetaStoreException> exceptionList ) throws MetaStoreException {
    return getElements( namespace, elementType, lock, true, exceptionList );
  }

  protected synchronized List<IMetaStoreElement> getElements( String namespace, IMetaStoreElementType elementType,
                                                              boolean lock, boolean includeProcessedFiles, List<MetaStoreException> exceptionList ) throws MetaStoreException {
    if ( lock ) {
      lockStore();
    }
    try {
      String elementTypeFolder = XmlUtil.getElementTypeFolder( rootFolder, namespace, elementType.getName() );

      FileObject elementTypeFolderFile = rootFile.resolveFile( elementTypeFolder );
      List<FileObject> elementTypeFiles = listFiles( elementTypeFolderFile, includeProcessedFiles );
      List<IMetaStoreElement> elements = new ArrayList<>( elementTypeFiles.size() );
      for ( FileObject elementTypeFile : elementTypeFiles ) {
        String elementId = elementTypeFile.getName().getBaseName();
        // File .type.xml doesn't hidden in OS Windows so better to ignore it explicitly
        if ( elementId.equals( XmlUtil.ELEMENT_TYPE_FILE_NAME ) ) {
          continue;
        }
        elementId = elementId.substring( 0, elementId.length() - 4 ); // remove .xml to get the ID
        try {
          elements.add( getElement( namespace, elementType, elementId, false ) );
        } catch ( Exception e ) {
          // If we are collecting exceptions instead of fatally exiting, add to the list of exceptions and continue
          if ( exceptionList != null ) {
            exceptionList.add( new MetaStoreException( "Could not load metaStore element '" + elementId + "'", e ) );
          } else {
            // Strict run. abort list
            throw e;
          }
        }
      }

      return elements;
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( ex );
    } finally {
      if ( lock ) {
        unlockStore();
      }
    }
  }

  @Override
  public synchronized List<String> getElementIds( String namespace, IMetaStoreElementType elementType )
    throws MetaStoreException {
    lockStore();
    try {
      String elementTypeFolder = XmlUtil.getElementTypeFolder( rootFolder, namespace, elementType.getName() );
      FileObject elementTypeFolderFile = rootFile.resolveFile( elementTypeFolder );
      List<FileObject> elementTypeFiles = listFiles( elementTypeFolderFile, true );
      List<String> elementIds = new ArrayList<>( elementTypeFiles.size() );
      for ( FileObject elementTypeFile : elementTypeFiles ) {
        String elementId = elementTypeFile.getName().getBaseName();
        // File .type.xml doesn't hidden in OS Windows so better to ignore it explicitly
        if ( elementId.equals( XmlUtil.ELEMENT_TYPE_FILE_NAME ) ) {
          continue;
        }
        elementId = elementId.substring( 0, elementId.length() - 4 ); // remove .xml to get the ID
        elementIds.add( elementId );
      }

      return elementIds;
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( ex );
    } finally {
      unlockStore();
    }
  }

  @Override
  public IMetaStoreElement getElement( String namespace, IMetaStoreElementType elementType, String elementId )
    throws MetaStoreException {
    return getElement( namespace, elementType, elementId, true );
  }

  protected synchronized IMetaStoreElement getElement( String namespace, IMetaStoreElementType elementType,
                                                       String elementId, boolean lock ) throws MetaStoreException {
    if ( lock ) {
      lockStore();
    }
    try {
      String elementFilename = XmlUtil.getElementFile( rootFolder, namespace, elementType.getName(), elementId );
      FileObject elementFile = rootFile.resolveFile( elementFilename );
      if ( !elementFile.exists() ) {
        return null;
      }
      XmlVfsMetaStoreElement element = new XmlVfsMetaStoreElement( elementFilename, elementFile.getContent().getInputStream() );
      metaStoreCache.registerElementIdForName( namespace, elementType, element.getName(), elementId );
      metaStoreCache.registerProcessedFile( elementFilename, elementFile.getContent().getLastModifiedTime() );
      return element;
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( ex );
    } finally {
      if ( lock ) {
        unlockStore();
      }
    }
  }

  @Override
  public IMetaStoreElement getElementByName( String namespace, IMetaStoreElementType elementType, String name )
    throws MetaStoreException {
    return getElementByName( namespace, elementType, name, true );
  }

  @Override
  public synchronized IMetaStoreElement getElementByName( String namespace, IMetaStoreElementType elementType, String name, boolean lock )
    throws MetaStoreException {
    if ( lock ) {
      lockStore();
    }
    try {
      String chachedElementId = metaStoreCache.getElementIdByName( namespace, elementType, name );
      if ( chachedElementId != null ) {
        IMetaStoreElement element = getElement( namespace, elementType, chachedElementId, false );
        if ( element != null && element.getName().equalsIgnoreCase( name ) ) {
          return element;
        }
      }

      for ( IMetaStoreElement element : getElements( namespace, elementType, false, false, new ArrayList<MetaStoreException>() ) ) {
        if ( element.getName() != null && element.getName().equalsIgnoreCase( name ) ) {
          return element;
        }
      }
      return null;
    } finally {
      if ( lock ) {
        unlockStore();
      }
    }
  }

  public synchronized void
  createElement( String namespace, IMetaStoreElementType elementType, IMetaStoreElement element )
    throws MetaStoreException {
    lockStore();
    try {
      // In the case of a file, the ID is the name
      //
      if ( element.getId() == null ) {
        element.setId( element.getName() );
      }

      String elementFilename = XmlUtil.getElementFile( rootFolder, namespace, elementType.getName(), element.getId() );
      FileObject elementFile = rootFile.resolveFile( elementFilename );
      if ( elementFile.exists() ) {
        throw new MetaStoreElementExistException( getElements( namespace, elementType, false, true, new ArrayList<MetaStoreException>() ),
                                                  "The specified element already exists with the same ID: '" + element.getId() + "'" );
      }
      XmlVfsMetaStoreElement xmlElement = new XmlVfsMetaStoreElement( element );
      xmlElement.setFilename( elementFilename );
      xmlElement.save( elementFile.getContent().getOutputStream() );

      metaStoreCache.registerElementIdForName( namespace, elementType, xmlElement.getName(), element.getId() );

      metaStoreCache.registerProcessedFile( elementFilename, elementFile.getContent().getLastModifiedTime() );
      // In the case of the XML store, the name is the same as the ID
      //
      element.setId( xmlElement.getName() );
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( ex );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized void updateElement( String namespace, IMetaStoreElementType elementType, String elementId,
                                          IMetaStoreElement element ) throws MetaStoreException {

    // verify that the element type belongs to this meta store
    //
    if ( elementType.getMetaStoreName() == null || !elementType.getMetaStoreName().equals( getName() ) ) {
      throw new MetaStoreException( "The element type '" + elementType.getName()
                                    + "' needs to explicitly belong to the meta store in which you are updating." );
    }

    lockStore();
    try {
      String elementFilename = XmlUtil.getElementFile( rootFolder, namespace, elementType.getName(), element.getName() );
      FileObject elementFile = rootFile.resolveFile( elementFilename );
      if ( !elementFile.exists() ) {
        throw new MetaStoreException( "The specified element to update doesn't exist with ID: '" + elementId + "'" );
      }

      XmlVfsMetaStoreElement xmlElement = new XmlVfsMetaStoreElement( element );
      xmlElement.setFilename( elementFilename );
      xmlElement.setIdWithFilename( elementFilename );
      xmlElement.save( elementFile.getContent().getOutputStream() );

      metaStoreCache.registerElementIdForName( namespace, elementType, xmlElement.getName(), xmlElement.getId() );
      metaStoreCache.registerProcessedFile( elementFilename, elementFile.getContent().getLastModifiedTime() );
    } catch ( FileSystemException  ex ) {
      throw new MetaStoreException( ex );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized void deleteElement( String namespace, IMetaStoreElementType elementType, String elementId )
    throws MetaStoreException {
    lockStore();
    try {
      String elementFilename = XmlUtil.getElementFile( rootFolder, namespace, elementType.getName(), elementId );
      FileObject elementFile = rootFile.resolveFile( elementFilename );
      if ( !elementFile.exists() ) {
        return;
      }

      if ( !elementFile.delete() ) {
        throw new MetaStoreException( "Unable to delete element with ID '" + elementId + "' in filename '"
                                      + elementFilename + "'" );
      }

      metaStoreCache.unregisterElementId( namespace, elementType, elementId );
      metaStoreCache.unregisterProcessedFile( elementFilename );
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( ex );
    } finally {
      unlockStore();
    }
  }

  /**
   * @return the rootFolder
   */
  public String getRootFolder() {
    return rootFolder;
  }

  /**
   * @param rootFolder
   *          the rootFolder to set
   */
  public void setRootFolder( String rootFolder ) {
    this.rootFolder = rootFolder;
  }

  /**
   * @param folder
   * @return the non-hidden folders in the specified folder
   */
  protected List<FileObject> listFolders( FileObject folder ) throws MetaStoreException {
    try {
      FileObject[] children = folder.getChildren();
      List<FileObject> folders = new ArrayList<>();

      for ( FileObject child : children ) {
        if ( child.isFolder() && !child.isHidden() ) {
          folders.add( child );
        }
      }
      return folders;
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( ex );
    }
  }

  /**
   * @param folder
   * @param includeProcessedFiles
   * @return the non-hidden files in the specified folder
   */
  protected List<FileObject> listFiles( FileObject folder, final boolean includeProcessedFiles ) throws MetaStoreException {
    try {
      FileObject[] files = folder.getChildren();
      List<FileObject> retlist = new ArrayList<>();

      if ( files == null ) {
        files = new FileObject[] { };
      }

      for ( FileObject file : files ) {
        if ( file.isFile() && !file.isHidden() ) {
          if ( !includeProcessedFiles ) {
            Map<String, Long> processedFiles = metaStoreCache.getProcessedFiles();
            Long fileLastModified = processedFiles.get( file.getName().getPath() );
            if ( fileLastModified == null || !fileLastModified.equals( file.getContent().getLastModifiedTime() ) ) {
              retlist.add( file );
            }
          } else {
            retlist.add( file );
          }
        }
      }
      return retlist;
    } catch ( FileSystemException ex ) {
      throw new MetaStoreException( ex );
    }
  }

  @Override
  public IMetaStoreElementType newElementType( String namespace ) throws MetaStoreException {
    return new XmlVfsMetaStoreElementType( namespace, null, null, null );
  }

  @Override
  public IMetaStoreElement newElement() throws MetaStoreException {
    return new XmlVfsMetaStoreElement();
  }

  @Override
  public IMetaStoreElement newElement( IMetaStoreElementType elementType, String id, Object value )
    throws MetaStoreException {
    return new XmlVfsMetaStoreElement( elementType, id, value );
  }

  @Override
  public IMetaStoreElementOwner newElementOwner( String name, MetaStoreElementOwnerType ownerType )
    throws MetaStoreException {
    return new XmlMetaStoreElementOwner( name, ownerType );
  }

  /**
   * Create a .lock file in the store root folder. If it already exists, wait until it becomes available.
   *
   * @throws MetaStoreException
   *           in case we have to wait more than 10 seconds to acquire a lock
   */
  protected void lockStore() throws MetaStoreException {
    //TODO
//  boolean waiting = true;
//  long totalTime = 0L;
//  while ( waiting ) {
//    FileObject lockFile = KettleVFS.getFileObject(vfsPath(rootFile + File.separator + ".lock" ))
//    try {
//      if ( lockFile.createNewFile() ) {
//        return;
//      }
//    } catch ( IOException e ) {
//      throw new MetaStoreException( "Unable to create lock file: " + lockFile.toString(), e );
//    }
//    try {
//      Thread.sleep( 100 );
//    } catch ( InterruptedException e ) {
//      throw new RuntimeException( e );
//    }
//    totalTime += 100;
//    if ( totalTime > 10000 ) {
//      throw new MetaStoreException( "Maximum wait time of 10 seconds exceed while acquiring lock" );
//    }
//  }
  }

  protected void unlockStore() {
    //TODO
//  File lockFile = new File( rootFile, ".lock" );
//  lockFile.delete();
  }
}
