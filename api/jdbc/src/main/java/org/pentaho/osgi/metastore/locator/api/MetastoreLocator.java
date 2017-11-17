package org.pentaho.osgi.metastore.locator.api;

/**
 * Created by Vasilina_Terehova on 11/16/2017.
 */
import org.pentaho.metastore.api.IMetaStore;

/**
 * Created by tkafalas on 6/19/2017
 */
public interface MetastoreLocator {
  final String LOCAL_PROVIDER_KEY = "LocalMetastoreProvider";
  final String REPOSITORY_PROVIDER_KEY = "RepositoryMetastoreProvider";

  /**
   * Attempts to pick the best the MetaStore based on environment; either
   * the local or repository metastore.
   * @return The metastore to use
   */
  IMetaStore getMetastore();

  /**
   * Works similar to (@link #getMetastore()) except that it will fall back to a metastore with a key of
   * providerKey if the both repository and local metastore both are not found.
   * @param providerKey
   * @return
   */
  IMetaStore getMetastore( String providerKey );

  /**
   * Registers a metastore provider that returns the received metastore with the current thread. {@link #getMetastore()}
   * will used this metastore iff the repository and local metastore provider cannot be found.
   * @param metastore
   * @return
   */
  String setEmbeddedMetastore( IMetaStore metastore );

  /**
   * Dispose a metastore provider assciated with the providerKey
   * @param providerKey The key to the metastore provider.
   */
  public void disposeMetastoreProvider( String providerKey );

  /**
   * Unconditionally returns the metastore stored with the given provider key, or null if it does not exist.
   * @param providerKey
   * @return
   */
  public IMetaStore getExplicitMetastore( String providerKey );
}