package mil.nga.giat.geowave.format.nyctlc.cli;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

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

@GeowaveOperation(name = "query", parentOperation = NYCTLCSection.class)
@Parameters(commandDescription = "multi-dimensional query")
public class NYCTLCQueryCommand extends
		DefaultOperation implements
		Command
{

	private final static Logger LOGGER = Logger.getLogger(NYCTLCQueryCommand.class);

	private static final SimpleDateFormat CQL_DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd'T'hh:mm:ss z");

	private static final SimpleDateFormat INPUT_DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd-hh:mm:ss");

	@Parameter(names = {
		"-pe",
		"--peast"
	}, required = true, description = "Max Longitude of BBOX")
	private Double peast;

	@Parameter(names = {
		"-pw",
		"--pwest"
	}, required = true, description = "Min Longitude of BBOX")
	private Double pwest;

	@Parameter(names = {
		"-pn",
		"--pnorth"
	}, required = true, description = "Max Latitude of BBOX")
	private Double pnorth;

	@Parameter(names = {
		"-ps",
		"--psouth"
	}, required = true, description = "Min Latitude of BBOX")
	private Double psouth;
	@Parameter(names = {
		"-de",
		"--deast"
	}, required = true, description = "Max Longitude of BBOX")
	private Double deast;

	@Parameter(names = {
		"-dw",
		"--dwest"
	}, required = true, description = "Min Longitude of BBOX")
	private Double dwest;

	@Parameter(names = {
		"-dn",
		"--dnorth"
	}, required = true, description = "Max Latitude of BBOX")
	private Double dnorth;

	@Parameter(names = {
		"-ds",
		"--dsouth"
	}, required = true, description = "Min Latitude of BBOX")
	private Double dsouth;

	@Parameter(names = {
		"-pst"
	}, required = true, description = "Min Latitude of BBOX")
	private String pstartTime;

	@Parameter(names = {
		"-pet"
	}, required = true, description = "Min Latitude of BBOX")
	private String pendTime;

	@Parameter(names = {
		"-dst"
	}, required = true, description = "Min Latitude of BBOX")
	private String dstartTime;

	@Parameter(names = {
		"-det"
	}, required = true, description = "Min Latitude of BBOX")
	private String dendTime;
	@Parameter(names = {
		"--useAggregation",
		"-agg"
	}, description = "Compute count on the server side")
	private final Boolean useAggregation = Boolean.FALSE;

	@Parameter(description = "<storename>")
	private final List<String> parameters = new ArrayList<String>();

	private Geometry pgeom;
	private Geometry dgeom;
	private Date pStart;
	private Date pEnd;
	private Date dStart;
	private Date dEnd;

	private void getBoxGeom()
			throws java.text.ParseException {
		pgeom = new GeometryFactory().toGeometry(new Envelope(
				pwest,
				peast,
				psouth,
				pnorth));
		dgeom = new GeometryFactory().toGeometry(new Envelope(
				dwest,
				deast,
				dsouth,
				dnorth));
		pStart = INPUT_DATE_FORMAT.parse(pstartTime);
		pEnd = INPUT_DATE_FORMAT.parse(pendTime);
		dStart = INPUT_DATE_FORMAT.parse(dstartTime);
		dEnd = INPUT_DATE_FORMAT.parse(dendTime);
	}

	@Override
	public void execute(
			final OperationParams params )
			throws ParseException {
		final Stopwatch stopWatch = new Stopwatch();

		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires arguments: <storename>");
		}

		final String storeName = parameters.get(0);

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
				getBoxGeom();
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

				final String cqlPredicate = "BBOX(\"" + NYCTLCUtils.Field.PICKUP_LOCATION.getIndexedName() + "\","
						+ pwest + "," + psouth + "," + peast + "," + pnorth + ") AND BBOX(\""
						+ NYCTLCUtils.Field.DROPOFF_LOCATION.getIndexedName() + "\"," + dwest + "," + dsouth + ","
						+ deast + "," + dnorth + ") AND \"" + NYCTLCUtils.Field.PICKUP_DATETIME.getIndexedName()
						+ "\" <= '" + CQL_DATE_FORMAT.format(pEnd) + "' AND \""
						+ NYCTLCUtils.Field.PICKUP_DATETIME.getIndexedName() + "\" >= '"
						+ CQL_DATE_FORMAT.format(pStart) + "' AND \""
						+ NYCTLCUtils.Field.DROPOFF_DATETIME.getIndexedName() + "\" <= '"
						+ CQL_DATE_FORMAT.format(dEnd) + "' AND \""
						+ NYCTLCUtils.Field.DROPOFF_DATETIME.getIndexedName() + "\" >= '"
						+ CQL_DATE_FORMAT.format(dStart) + "'";
				System.err.println(cqlPredicate);
				try {
					query = CQLQuery.createOptimalQuery(
							cqlPredicate,
							adapter,
							index);
				}
				catch (CQLException e) {
					e.printStackTrace();
				}
			}
			stopWatch.start();
			final long results = runQuery(
					adapter,
					adapter.getAdapterId(),
					index,
					dataStore,
					query);
			stopWatch.stop();
			System.out.println("Got " + results + " results in " + stopWatch.toString());
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read adapter",
					e);
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
}
