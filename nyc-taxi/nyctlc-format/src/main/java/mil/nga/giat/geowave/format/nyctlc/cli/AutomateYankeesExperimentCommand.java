package mil.nga.giat.geowave.format.nyctlc.cli;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.geotools.filter.text.cql2.CQLException;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCIngestPlugin;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCOptionProvider;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils;
import mil.nga.giat.geowave.format.nyctlc.query.MultiGeoMultiTimestampQuery;
import mil.nga.giat.geowave.format.nyctlc.query.MultiGeoTimeRangeQuery;

@GeowaveOperation(name = "yankees", parentOperation = NYCTLCSection.class)
@Parameters(commandDescription = "multi-dimensional query, yankees experiment automated")
public class AutomateYankeesExperimentCommand extends
		DefaultOperation implements
		Command
{

	private static YankeesExperiment[] experiments = new YankeesExperiment[] {
		new YankeesExperiment(
				-73.575,
				-74.375,
				41.1,
				40.3,
				-73.921,
				-73.931,
				40.835,
				40.826,
				"2009-04-05-00:00:00",
				"2009-11-04-23:59:59",
				"2009-04-05-00:00:00",
				"2009-11-04-23:59:59",
				"2009-04-05-05:00:00",
				"2009-11-05-04:59:59",
				"2009-04-05-05:00:00",
				"2009-11-05-04:59:59",
				"dropoff_season"),
		new YankeesExperiment(
				-73.921,
				-73.931,
				40.835,
				40.826,
				-73.575,
				-74.375,
				41.1,
				40.3,
				"2009-04-05-00:00:00",
				"2009-11-04-23:59:59",
				"2009-04-05-00:00:00",
				"2009-11-04-23:59:59",
				"2009-04-05-05:00:00",
				"2009-11-05-04:59:59",
				"2009-04-05-05:00:00",
				"2009-11-05-04:59:59",
				"pickup_season"),
		new YankeesExperiment(
				-73.575,
				-74.375,
				41.1,
				40.3,
				-73.921,
				-73.931,
				40.835,
				40.826,
				"2009-11-06-00:00:00",
				"2010-04-03-23:59:59",
				"2009-11-06-00:00:00",
				"2010-04-03-23:59:59",
				"2009-11-06-05:00:00",
				"2010-04-04-04:59:59",
				"2009-11-06-05:00:00",
				"2010-04-04-04:59:59",
				"dropoff_offseason"),
		new YankeesExperiment(
				-73.921,
				-73.931,
				40.835,
				40.826,
				-73.575,
				-74.375,
				41.1,
				40.3,
				"2009-11-06-00:00:00",
				"2010-04-03-23:59:59",
				"2009-11-06-00:00:00",
				"2010-04-03-23:59:59",
				"2009-11-06-05:00:00",
				"2010-04-04-04:59:59",
				"2009-11-06-05:00:00",
				"2010-04-04-04:59:59",
				"pickup_offseason"),
		new YankeesExperiment(
				-73.575,
				-74.375,
				41.1,
				40.3,
				-73.921,
				-73.931,
				40.835,
				40.826,
				"2009-10-28-00:00:00",
				"2009-11-04-23:59:59",
				"2009-10-28-00:00:00",
				"2009-11-04-23:59:59",
				"2009-10-28-05:00:00",
				"2009-11-05-04:59:59",
				"2009-10-28-05:00:00",
				"2009-11-05-04:59:59",
				"dropoff_world_series"),
		new YankeesExperiment(
				-73.921,
				-73.931,
				40.835,
				40.826,
				-73.575,
				-74.375,
				41.1,
				40.3,
				"2009-04-05-00:00:00",
				"2009-11-04-23:59:59",
				"2009-04-05-00:00:00",
				"2009-11-04-23:59:59",
				"2009-04-05-05:00:00",
				"2009-11-05-04:59:59",
				"2009-04-05-05:00:00",
				"2009-11-05-04:59:59",
				"pickup_world_series"),
		new YankeesExperiment(
				-73.575,
				-74.375,
				41.1,
				40.3,
				-73.921,
				-73.931,
				40.835,
				40.826,
				"2009-11-04-00:00:00",
				"2009-11-04-23:59:59",
				"2009-11-04-00:00:00",
				"2009-11-04-23:59:59",
				"2009-11-04-05:00:00",
				"2009-11-05-04:59:59",
				"2009-11-04-05:00:00",
				"2009-11-05-04:59:59",
				"dropoff_final_game"),
		new YankeesExperiment(
				-73.921,
				-73.931,
				40.835,
				40.826,
				-73.575,
				-74.375,
				41.1,
				40.3,
				"2009-11-04-00:00:00",
				"2009-11-04-23:59:59",
				"2009-11-04-00:00:00",
				"2009-11-04-23:59:59",
				"2009-11-04-05:00:00",
				"2009-11-05-04:59:59",
				"2009-11-04-05:00:00",
				"2009-11-05-04:59:59",
				"pickup_final_game"),
	};

	private final static Logger LOGGER = Logger.getLogger(AutomateYankeesExperimentCommand.class);

	private static final SimpleDateFormat CQL_DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd'T'hh:mm:ss z");

	private static final SimpleDateFormat INPUT_DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd-hh:mm:ss");

	@Parameter(names = {
		"--useAggregation",
		"-agg"
	}, description = "Compute count on the server side")
	private final Boolean useAggregation = Boolean.FALSE;

	@Parameter(names = {
		"--numSamples",
		"-s"
	}, description = "Number of samples to compute")
	private int samples = 5;
	@Parameter(description = "<storename>")
	private List<String> parameters = new ArrayList<String>();

	private Geometry pgeom;
	private Geometry dgeom;
	private Date pStart;
	private Date pEnd;
	private Date dStart;
	private Date dEnd;

	private void getBoxGeom(
			YankeesExperiment e,
			String storeName )
			throws java.text.ParseException {
		pgeom = new GeometryFactory().toGeometry(new Envelope(
				e.pwest,
				e.peast,
				e.psouth,
				e.pnorth));
		dgeom = new GeometryFactory().toGeometry(new Envelope(
				e.dwest,
				e.deast,
				e.dsouth,
				e.dnorth));
		String pst = (storeName.contains("pickup") || storeName.contains("dropoff")) ? e.pstartTime2 : e.pstartTime1;
		String pet = (storeName.contains("pickup") || storeName.contains("dropoff")) ? e.pendTime2 : e.pendTime1;
		String dst = (storeName.contains("pickup") || storeName.contains("dropoff")) ? e.dstartTime2 : e.dstartTime1;
		String det = (storeName.contains("pickup") || storeName.contains("dropoff")) ? e.dendTime2 : e.dendTime1;
		pStart = INPUT_DATE_FORMAT.parse(pst);
		pEnd = INPUT_DATE_FORMAT.parse(pet);
		dStart = INPUT_DATE_FORMAT.parse(dst);
		dEnd = INPUT_DATE_FORMAT.parse(det);
	}

	@Override
	public void execute(
			final OperationParams params )
			throws ParseException {
		final Stopwatch stopWatch = new Stopwatch();

		// Ensure we have all the required arguments
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Requires arguments: <storename>");
		}
		for (YankeesExperiment e : experiments) {
			for (String storeName : parameters) {
				if ((storeName.equals("pickup_spatial2") && e.experimentName.contains("dropoff"))
						|| (storeName.equals("dropoff_spatial") && e.experimentName.contains("pickup"))) {
					continue;
				}
				// Attempt to load store.
				final StoreLoader storeOptions = new StoreLoader(
						storeName);
				if (!storeOptions.loadFromConfig(getGeoWaveConfigFile(params))) {
					throw new ParameterException(
							"Cannot find store name: " + storeOptions.getStoreName());
				}

				DataStore dataStore;
				AdapterStore adapterStore;
				IndexStore indexStore;
				try {
					dataStore = storeOptions.createDataStore();
					adapterStore = storeOptions.createAdapterStore();
					indexStore = storeOptions.createIndexStore();
					final CloseableIterator<Index<?, ?>> indexIt = indexStore.getIndices();
					final PrimaryIndex index = (PrimaryIndex) indexIt.next();
					indexIt.close();
					GeotoolsFeatureDataAdapter adapter;
					final CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
					adapter = (GeotoolsFeatureDataAdapter) it.next();
					it.close();
					if (storeName.contains("dropoff")) {
						NYCTLCOptionProvider o = new NYCTLCOptionProvider();
						o.dropoff = true;
						adapter = (GeotoolsFeatureDataAdapter) new NYCTLCIngestPlugin(
								o).getDataAdapters(null)[0];
					}
					try {
						getBoxGeom(
								e,
								storeName);
					}
					catch (java.text.ParseException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					Query query = null;
					if (index.getIndexModel().getDimensions().length > 3) {
						if (index.getIndexModel().getDimensions().length == 5) {
							query = new MultiGeoTimeRangeQuery(
									pStart,
									dEnd,
									pgeom,
									dgeom);
						}
						else {
							query = new MultiGeoMultiTimestampQuery(
									pStart,
									pEnd,
									dStart,
									dEnd,
									pgeom,
									dgeom);
						}
					}
					else {

						final String cqlPredicate = "BBOX(\"" + NYCTLCUtils.Field.PICKUP_LOCATION.getIndexedName()
								+ "\"," + e.pwest + "," + e.psouth + "," + e.peast + "," + e.pnorth + ") AND BBOX(\""
								+ NYCTLCUtils.Field.DROPOFF_LOCATION.getIndexedName() + "\"," + e.dwest + ","
								+ e.dsouth + "," + e.deast + "," + e.dnorth + ") AND \""
								+ NYCTLCUtils.Field.PICKUP_DATETIME.getIndexedName() + "\" <= '"
								+ CQL_DATE_FORMAT.format(pEnd) + "' AND \""
								+ NYCTLCUtils.Field.PICKUP_DATETIME.getIndexedName() + "\" >= '"
								+ CQL_DATE_FORMAT.format(pStart) + "' AND \""
								+ NYCTLCUtils.Field.DROPOFF_DATETIME.getIndexedName() + "\" <= '"
								+ CQL_DATE_FORMAT.format(dEnd) + "' AND \""
								+ NYCTLCUtils.Field.DROPOFF_DATETIME.getIndexedName() + "\" >= '"
								+ CQL_DATE_FORMAT.format(dStart) + "'";
						try {
							query = CQLQuery.createOptimalQuery(
									cqlPredicate,
									adapter,
									index);
						}
						catch (CQLException ex) {
							ex.printStackTrace();
						}
					}
					double[] data = new double[samples];
					long results = 0;
					for (int i = 0; i < samples; i++) {
						stopWatch.start();
						results = runQuery(
								adapter,
								adapter.getAdapterId(),
								index,
								dataStore,
								query);
						stopWatch.stop();
						data[i] = stopWatch.elapsed(TimeUnit.MILLISECONDS);
					}
					new Statistics(
							data,
							results,
							storeName,
							e.experimentName).printStats();
				}
				catch (final IOException ex) {
					LOGGER.warn(
							"Unable to read adapter",
							ex);
				}
			}
		}
	}

	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final PrimaryIndex index,
			final DataStore dataStore,
			Query query ) {
		final Stopwatch stopWatch = new Stopwatch();

		long count = 0;
		if (useAggregation) {
			final QueryOptions options = new QueryOptions(
					adapterId,
					index.getId());
			options.setAggregation(
					new CountAggregation(),
					adapter);

			try (final CloseableIterator<Object> it = dataStore.query(
					options,
					query)) {
				final CountResult result = ((CountResult) (it.next()));
				if (result != null) {
					count += result.getCount();
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to read result",
						e);
			}
		}
		else {
			stopWatch.start();

			final CloseableIterator<Object> it = dataStore.query(
					new QueryOptions(
							adapterId,
							index.getId()),
					query);

			stopWatch.stop();
			System.out.println("Ran 6-dimensional query in " + stopWatch.toString());

			stopWatch.reset();
			stopWatch.start();

			while (it.hasNext()) {
				it.next();
				count++;
			}

			stopWatch.stop();
			System.out.println("BBOX query results iteration took " + stopWatch.toString());
		}
		return count;
	}

	private static class YankeesExperiment
	{
		private Double peast;

		private Double pwest;

		private Double pnorth;

		private Double psouth;

		private Double deast;

		private Double dwest;

		private Double dnorth;

		private Double dsouth;

		private String pstartTime1;

		private String pendTime1;

		private String dstartTime1;
		private String dendTime1;
		private String pstartTime2;

		private String pendTime2;

		private String dstartTime2;
		private String dendTime2;
		private String experimentName;

		public YankeesExperiment(
				Double peast,
				Double pwest,
				Double pnorth,
				Double psouth,
				Double deast,
				Double dwest,
				Double dnorth,
				Double dsouth,
				String pstartTime1,
				String pendTime1,
				String dstartTime1,
				String dendTime1,
				String pstartTime2,
				String pendTime2,
				String dstartTime2,
				String dendTime2,
				String experimentName ) {
			super();
			this.peast = peast;
			this.pwest = pwest;
			this.pnorth = pnorth;
			this.psouth = psouth;
			this.deast = deast;
			this.dwest = dwest;
			this.dnorth = dnorth;
			this.dsouth = dsouth;
			this.pstartTime1 = pstartTime1;
			this.pendTime1 = pendTime1;
			this.dstartTime1 = dstartTime1;
			this.dendTime1 = dendTime1;
			this.pstartTime2 = pstartTime2;
			this.pendTime2 = pendTime2;
			this.dstartTime2 = dstartTime2;
			this.dendTime2 = dendTime2;
			this.experimentName = experimentName;
		}
	}
}
