package mil.nga.giat.geowave.process.nyctlc;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.opengis.feature.simple.SimpleFeature;

import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.analytics.spark.GeoWaveContext;
import mil.nga.giat.geowave.analytics.spark.GeoWaveRDD;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreStatisticsProvider;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCIngestPlugin;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class CalculateStats
{
	public static void main(
			final String[] args ) {

		DataStorePluginOptions inputStoreOptions = null;
		// Attempt to load input store.
		if (inputStoreOptions == null) {
			final StoreLoader inputStoreLoader = new StoreLoader(
					args[0]);
			if (!inputStoreLoader.loadFromConfig(ConfigOptions.getDefaultPropertyFile())) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		;
		// Load the Indexes
		final IndexLoader indexLoader = new IndexLoader(
				args[1]);
		if (!indexLoader.loadFromConfig(ConfigOptions.getDefaultPropertyFile())) {
			throw new ParameterException(
					"Cannot find index(s) by name: " + args[2]);
		}
		final List<IndexPluginOptions> inputIndexOptions = indexLoader.getLoadedIndexes();

		final PrimaryIndex index = inputIndexOptions.get(
				0).createPrimaryIndex();
		final byte[] indexBinary = PersistenceUtils.toBinary(index);
		final DataStore store = inputStoreOptions.createDataStore();
		final WritableDataAdapter a = new NYCTLCIngestPlugin().getDataAdapters("")[0];
		try {
			final IndexWriter writer = store.createWriter(
					a,
					index);
			writer.write(null);
		}
		catch (final Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		SparkContext sc = new SparkContext();
		final JavaPairRDD geowaveRdd = JavaPairRDD.fromRDD(
				GeoWaveRDD.rddForSimpleFeatures(
						sc,
						new QueryOptions(),
						256,
						256,
						// new CQLQuery(
						// baseQuery,
						// filter,
						// adapter),
						null,
						GeoWaveContext.apply(
								inputStoreOptions,
								inputStoreOptions.getType(),
								inputStoreOptions.getGeowaveNamespace())),
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(GeoWaveInputKey.class),
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(SimpleFeature.class));

		final Map<String, String> optionsMap = inputStoreOptions.getOptionsAsMap();
		optionsMap.put(
				GeoWaveStoreFinder.STORE_HINT_OPTION.getName(),
				inputStoreOptions.getType());
		geowaveRdd.foreachPartition(new StatsCalc(
				indexBinary,
				optionsMap));
		sc.stop();
	}

	private static class StatsCalc implements
			VoidFunction<Iterator<Tuple2<GeoWaveInputKey, SimpleFeature>>>
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		private final byte[] indexBinary;
		private final Map<String, String> configOptions;

		public StatsCalc(
				final byte[] indexBinary,
				final Map<String, String> configOptions ) {
			super();
			this.indexBinary = indexBinary;
			this.configOptions = configOptions;
		}

		@Override
		public void call(
				final Iterator<Tuple2<GeoWaveInputKey, SimpleFeature>> it )
				throws Exception {
			final PrimaryIndex index = PersistenceUtils.fromBinary(
					indexBinary,
					PrimaryIndex.class);
			final WritableDataAdapter a = new NYCTLCIngestPlugin().getDataAdapters("")[0];
			try (StatsCompositionTool statsAggregator = new StatsCompositionTool(
					new DataStoreStatisticsProvider(
							a,
							index,
							true),
					GeoWaveStoreFinder.createDataStatisticsStore(configOptions))) {
				while (it.hasNext()) {
					final Tuple2<GeoWaveInputKey, SimpleFeature> next = it.next();
					statsAggregator.entryIngested(
							DataStoreUtils.getIngestInfo(
									a,
									index,
									next._2,
									DataStoreUtils.UNCONSTRAINED_VISIBILITY),
							next._2);
				}
			}
		}
	}
}
