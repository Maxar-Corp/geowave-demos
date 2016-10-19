package mil.nga.giat.geowave.process.nyctlc;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.ConnectorPool;

public class AccumuloImportFromS3
{
	public static void main(
			final String[] args ) {

		DataStorePluginOptions inputStoreOptions = null;
		// Attempt to load input store.
		if (inputStoreOptions == null) {
			final StoreLoader inputStoreLoader = new StoreLoader(
					args[2]);
			if (!inputStoreLoader.loadFromConfig(ConfigOptions.getDefaultPropertyFile())) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		// Load the Indexes
		final IndexLoader indexLoader = new IndexLoader(
				args[3]);

		if (!indexLoader.loadFromConfig(ConfigOptions.getDefaultPropertyFile())) {
			throw new ParameterException(
					"Cannot find index(s) by name: " + args[2]);
		}
		final List<IndexPluginOptions> inputIndexOptions = indexLoader.getLoadedIndexes();
		final PrimaryIndex index = inputIndexOptions.get(
				0).createPrimaryIndex();
		final AccumuloRequiredOptions options = (AccumuloRequiredOptions) inputStoreOptions.getFactoryOptions();

		try {
			final Connector c = ConnectorPool.getInstance().getConnector(
					options.getZookeeper(),
					options.getInstance(),
					options.getUser(),
					options.getPassword());
			c.tableOperations().importDirectory(
					AccumuloUtils.getQualifiedTableName(
							options.getGeowaveNamespace(),
							index.getId().getString()),
					args[0],
					args[1],
					true);
		}
		catch (TableNotFoundException | IOException | AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
