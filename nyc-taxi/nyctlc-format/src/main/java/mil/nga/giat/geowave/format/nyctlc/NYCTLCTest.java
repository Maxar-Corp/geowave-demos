package mil.nga.giat.geowave.format.nyctlc;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialTemporalQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.format.nyctlc.adapter.NYCTLCDataAdapter;
import mil.nga.giat.geowave.format.nyctlc.ingest.NYCTLCDimensionalityTypeProvider;
import mil.nga.giat.geowave.format.nyctlc.query.NYCTLCAggregation;
import mil.nga.giat.geowave.format.nyctlc.query.NYCTLCQuery;
import mil.nga.giat.geowave.format.nyctlc.statistics.NYCTLCParameters;
import mil.nga.giat.geowave.format.nyctlc.statistics.NYCTLCStatistics;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.GeometryBuilder;
import org.opengis.feature.simple.SimpleFeature;

import java.util.Date;

public class NYCTLCTest
{

	public static void main(
			String[] args )
			throws Exception {
		// normalIndexTest();
		newIndexTest();
	}

	public static void normalIndexTest()
			throws Exception {

		final AccumuloOperations operations = new BasicAccumuloOperations(
				"localhost:2181",
				"geowave",
				"root",
				"geowave",
				"spatial");

		final AccumuloDataStore dataStore = new AccumuloDataStore(
				operations);

		IndexWriter<SimpleFeature> featureWriter = dataStore.createWriter(
				new FeatureDataAdapter(
						new NYCTLCIngestPlugin().getTypes()[0]),
				new SpatialTemporalDimensionalityTypeProvider().createPrimaryIndex());

		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				new NYCTLCIngestPlugin().getTypes()[0]);

		builder.set(
				NYCTLCUtils.Field.PICKUP_LOCATION.getIndexedName(),
				new WKTReader().read("POINT(-73.961990356445313 40.715862274169922)"));
		builder.set(
				NYCTLCUtils.Field.TIME_OF_DAY_SEC.getIndexedName(),
				0);

		featureWriter.write(builder.buildFeature(null));

		final QueryOptions queryOptions = new QueryOptions();

		queryOptions.setIndex(new SpatialTemporalDimensionalityTypeProvider().createPrimaryIndex());

		final Query query = new SpatialTemporalQuery(
				new Date(
						0),
				new Date(
						5),
				new WKTReader().read("POINT(-73.961990356445313 40.715862274169922)"));

		final CloseableIterator<SimpleFeature> results = dataStore.query(
				queryOptions,
				query);

		while (results.hasNext()) {
			final SimpleFeature feature = results.next();

			System.out.println(feature);
		}

		results.close();
	}

	public static void newIndexTest()
			throws Exception {

		final AccumuloOperations operations = new BasicAccumuloOperations(
				"10.0.0.55:2181",
				"accumulo",
				"demo",
				"demo",
				"demo.nyctlc");

		final AccumuloDataStore dataStore = new AccumuloDataStore(
				operations);

		final QueryOptions queryOptions = new QueryOptions();

		queryOptions.setIndex(new NYCTLCDimensionalityTypeProvider().createPrimaryIndex());

		GeometryBuilder bdr = new GeometryBuilder();
		Geometry startGeom = bdr.circle(
				-73.954818725585937,
				40.820701599121094,
				0.005,
				20);

		bdr = new GeometryBuilder();
		Geometry destGeom = bdr.circle(
				-73.998832702636719,
				40.729896545410156,
				0.005,
				20);
		NYCTLCQuery query = new NYCTLCQuery(
				1,
				500,
				startGeom,
				destGeom);
		queryOptions.setAggregation(
				new NYCTLCAggregation(),
				new NYCTLCDataAdapter(
						new NYCTLCIngestPlugin().getTypes()[0]));

		if (queryOptions.getAggregation() != null) {
			final CloseableIterator<NYCTLCStatistics> results = dataStore.query(
					queryOptions,
					query);

			while (results.hasNext()) {
				final NYCTLCStatistics stats = results.next();

				System.out.println(stats.toJSONObject().toString(
						2));
			}
			results.close();
		}
		else {
			final CloseableIterator<SimpleFeature> results = dataStore.query(
					queryOptions,
					query);

			while (results.hasNext()) {
				final SimpleFeature feature = results.next();

				NYCTLCStatistics stats = new NYCTLCStatistics();
				stats.updateStats(
						feature,
						new NYCTLCParameters());
				byte[] statsBytes = stats.toBinary();

				NYCTLCStatistics statsFromBytes = new NYCTLCStatistics();
				statsFromBytes.fromBinary(statsBytes);

				System.out.println(stats.equals(statsFromBytes));

				System.out.println(feature);
			}
			results.close();
		}
	}
}