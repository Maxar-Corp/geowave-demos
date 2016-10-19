package mil.nga.giat.geowave.process.nyctlc;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.mortbay.log.Log;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.beust.jcommander.ParameterException;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloKeyValuePair;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloKeyValuePairGenerator;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCIngestPlugin;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils.Field;
import mil.nga.giat.geowave.format.nyctlc.avro.NYCTLCEntry;
import mil.nga.giat.geowave.format.nyctlc.ingest.NYCTLCDimensionalityTypeProvider;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class SparkIngest
{
	private final static Logger LOGGER = Logger.getLogger(SparkIngest.class);

	private final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private final static String DATE_START_FORMAT = "yyyyMMdd";

	public static void main(
			final String[] args ) {
		final SparkSession spark = SparkSession.builder().appName(
				"nyctlc ingest").config(
				"spark.serializer",
				"org.apache.spark.serializer.KryoSerializer").config(
				"spark.kryo.registrator",
				GeoWaveRegistrator.class.getCanonicalName())
		// .config(
		// "spark.sql.warehouse.dir",
		// "file:///C:/Temp/spark-warehouse")
		// .master(
		// "local")
				.getOrCreate();

		// Load the Indexes
		// final IndexLoader indexLoader = new IndexLoader(
		// args[2]);
		// if
		// (!indexLoader.loadFromConfig(ConfigOptions.getDefaultPropertyFile()))
		// {
		// throw new ParameterException(
		// "Cannot find index(s) by name: " + args[1]);
		// }
		// final List<IndexPluginOptions> inputIndexOptions =
		// indexLoader.getLoadedIndexes();
		int year = 2013;
		final List<String> paths = new ArrayList<>();
		for (int month = 8; month <= 12; month++) {
			paths.add(String.format(
					"s3://nyc-tlc/trip data/green_tripdata_%04d-%02d.csv",
					year,
					month));
		}
		for (year = 2014; year <= 2015; year++) {
			for (int month = 1; month <= 12; month++) {
				paths.add(String.format(
						"s3://nyc-tlc/trip data/green_tripdata_%04d-%02d.csv",
						year,
						month));
			}
		}
		year = 2016;
		for (int month = 1; month <= 6; month++) {
			paths.add(String.format(
					"s3://nyc-tlc/trip data/green_tripdata_%04d-%02d.csv",
					year,
					month));
		}
		for (year = 2009; year <= 2015; year++) {
			for (int month = 1; month <= 12; month++) {
				paths.add(String.format(
						"s3://nyc-tlc/trip data/yellow_tripdata_%04d-%02d.csv",
						year,
						month));
			}
		}
		year = 2016;
		for (int month = 1; month <= 6; month++) {
			paths.add(String.format(
					"s3://nyc-tlc/trip data/yellow_tripdata_%04d-%02d.csv",
					year,
					month));
		}
		final Dataset<Row> df = spark.read().format(
				"csv").option(
				"header",
				"true").option(
				"nullValue",
				"").csv(
				paths.toArray(new String[] {}));
		// "C:\\Users\\rfecher\\Downloads\\green_tripdata_2013-08.csv");
		// final PrimaryIndex index = inputIndexOptions.get(
		// 0).createPrimaryIndex();
		final PrimaryIndex index = new NYCTLCDimensionalityTypeProvider().createPrimaryIndex();
		final byte[] indexBinary = PersistenceUtils.toBinary(index);
		final RDD<Tuple2<Key, Value>> rdd = df.mapPartitions(
				new IngestFunction2(
						indexBinary),
				Encoders.tuple(
						Encoders.kryo(Key.class),
						Encoders.kryo(Value.class))).rdd();
		final JavaPairRDD javaRdd = JavaPairRDD.fromRDD(
				rdd,
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(Key.class),
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(Value.class));

		final Configuration conf = new Configuration();
		javaRdd.sortByKey().saveAsNewAPIHadoopFile(
				args[0],
				Key.class,
				Value.class,
				AccumuloFileOutputFormat.class,
				conf);
		spark.stop();
	}

	public static class IngestFunction2 implements
			MapPartitionsFunction<Row, scala.Tuple2<Key, Value>>
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1L;
		private final byte[] indexBinary;

		public IngestFunction2(
				final byte[] indexBinary ) {
			super();
			this.indexBinary = indexBinary;
		}

		@Override
		public Iterator<scala.Tuple2<Key, Value>> call(
				final Iterator<Row> arg0 )
				throws Exception {
			final PrimaryIndex index = PersistenceUtils.fromBinary(
					indexBinary,
					PrimaryIndex.class);
			final WritableDataAdapter a = new NYCTLCIngestPlugin().getDataAdapters("")[0];
			final SimpleDateFormat dateFormat = new SimpleDateFormat(
					DATE_FORMAT);
			final SimpleDateFormat dateStartFormat = new SimpleDateFormat(
					DATE_START_FORMAT);
			final AccumuloKeyValuePairGenerator<SimpleFeature> generator = new AccumuloKeyValuePairGenerator<SimpleFeature>(
					a,
					index,
					DataStoreUtils.UNCONSTRAINED_VISIBILITY);
			return Iterators.filter(
					Iterators.transform(
							arg0,
							new RowToKV(
									generator,
									a.getAdapterId().getBytes(),
									dateFormat,
									dateStartFormat)),
					new Predicate() {

						@Override
						public boolean apply(
								final Object input ) {
							return input != null;
						}
					});
		}

	}

	public static class RowToKV implements
			Function<Row, scala.Tuple2<Key, Value>>
	{
		AccumuloKeyValuePairGenerator<SimpleFeature> generator;
		private final RowToFeature rToF;
		private final byte[] adapterId;

		public RowToKV(
				final AccumuloKeyValuePairGenerator<SimpleFeature> generator,
				final byte[] adapterId,
				final SimpleDateFormat dateFormat,
				final SimpleDateFormat dateStartFormat ) {
			this.generator = generator;
			this.adapterId = adapterId;
			rToF = new RowToFeature(
					dateFormat,
					dateStartFormat,
					false);
		}

		@Override
		public scala.Tuple2<Key, Value> apply(
				final Row input ) {
			final SimpleFeature f = rToF.apply(input);
			if (f == null) {
				return null;
			}
			final List<AccumuloKeyValuePair> pairs = generator.constructKeyValuePairs(
					adapterId,
					f);
			if (!pairs.isEmpty()) {
				final AccumuloKeyValuePair pair = pairs.get(0);
				return new scala.Tuple2<Key, Value>(
						pair.getKey(),
						pair.getValue());
			}
			return null;

		}
	}

	public static class RowToFeature implements
			Function<Row, SimpleFeature>,
			org.apache.spark.api.java.function.Function<Row, SimpleFeature>
	{
		private final SimpleDateFormat dateFormat;
		private final SimpleDateFormat dateStartFormat;
		private boolean transformToWebMercator;
		private static final ThreadLocal<MathTransform> transform = new ThreadLocal<MathTransform>() {

			@Override
			protected MathTransform initialValue() {
				try {
					// create reference system WGS84 Web Mercator
					CoordinateReferenceSystem wgs84Web = CRS.decode(
							"EPSG:3857",
							true);
					// create reference system WGS84
					CoordinateReferenceSystem wgs84 = CRS.decode(
							"EPSG:4326",
							true);

					// Create transformation from WGS84 to WS84 Web Mercator

					MathTransform wgs84ToWgs84Web = CRS.findMathTransform(
							wgs84,
							wgs84Web);
					return wgs84ToWgs84Web;
				}
				catch (FactoryException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return super.initialValue();
			}

		};

		public RowToFeature(
				final SimpleDateFormat dateFormat,
				final SimpleDateFormat dateStartFormat,
				boolean transformToWebMercator ) {
			super();
			this.dateFormat = dateFormat;
			this.dateStartFormat = dateStartFormat;
			this.transformToWebMercator = transformToWebMercator;
		}

		public RowToFeature(
				boolean transformToWebMercator ) {
			dateFormat = new SimpleDateFormat(
					DATE_FORMAT);
			dateStartFormat = new SimpleDateFormat(
					DATE_START_FORMAT);
			this.transformToWebMercator = transformToWebMercator;
		}

		@Override
		public SimpleFeature apply(
				final Row input ) {
			final NYCTLCEntry pt = new NYCTLCEntry();

			pt.setCabType(2);
			// Sets the cabType beforehand because there is no cabType
			// column in the table
			final Set<Field> requiredFieldsMissing = new HashSet<Field>(
					NYCTLCUtils.REQUIRED_FIELDS);
			for (int fieldIdx = 0; fieldIdx < input.length(); fieldIdx++) {
				final String val = input.getString(fieldIdx);
				final NYCTLCUtils.Field field = NYCTLCUtils.Field.getField(input.schema().fieldNames()[fieldIdx]);
				boolean success = true;
				if ((field != null) && (val != null)) {
					try {
						switch (field) {
							case VENDOR_ID:
								if (val.equals("1") || val.equals("2")) {
									pt.setVendorId(Integer.parseInt(val));
								}
								else {
									if (val.toLowerCase().equals(
											"cmt")) {
										pt.setVendorId(1);
										requiredFieldsMissing.remove(field);
									}
									else if (val.toLowerCase().equals(
											"vts")) {
										pt.setVendorId(2);
										requiredFieldsMissing.remove(field);
									}
									else {
										success = false;
										pt.setVendorId(0); // unknown
									}
								}
								break;
							case PICKUP_DATETIME:
								try {
									final Date pickupDate = dateFormat.parse(val);
									pt.setPickupDatetime(pickupDate.getTime());

									final Date pickupDateStart = dateStartFormat.parse(dateStartFormat
											.format(pickupDate));
									pt.setTimeOfDaySec(new Long(
											TimeUnit.MILLISECONDS.toSeconds(pickupDate.getTime()
													- pickupDateStart.getTime())).longValue());

								}
								catch (final ParseException e) {
									success = false;
									Log.warn(
											"Error parsing pickup datetime: " + val,
											e);
								}
								break;
							case DROPOFF_DATETIME:
								try {
									pt.setDropoffDatetime(dateFormat.parse(
											val).getTime());
								}
								catch (final ParseException e) {
									success = false;
									Log.warn(
											"Error parsing dropoff datetime: " + val,
											e);
								}
								break;
							case PASSENGER_COUNT:
								pt.setPassengerCount(Integer.parseInt(val));
								break;
							case TRIP_DISTANCE:
								pt.setTripDistance(Double.parseDouble(val));
								break;
							case PICKUP_LONGITUDE:
								final double longitude = Double.parseDouble(val);
								if ((longitude > NYCTLCDimensionalityTypeProvider.MAX_LON)
										|| (longitude < NYCTLCDimensionalityTypeProvider.MIN_LON)) {
									success = false;
									break;
								}
								pt.setPickupLongitude(longitude);
								break;
							case PICKUP_LATITUDE:
								final double latitude = Double.parseDouble(val);
								if ((latitude > NYCTLCDimensionalityTypeProvider.MAX_LAT)
										|| (latitude < NYCTLCDimensionalityTypeProvider.MIN_LAT)) {
									success = false;
									break;
								}
								pt.setPickupLatitude(latitude);
								break;
							case PICKUP_LOCATION:
								// do nothing
								break;
							case STORE_AND_FWD_FLAG:
								final String strFwdFlg = val.toLowerCase();
								pt.setStoreAndFwdFlag(strFwdFlg.equals("1") || strFwdFlg.equals("y"));
								break;
							case RATE_CODE_ID:
								pt.setRateCodeId(Integer.parseInt(val));
								break;
							case DROPOFF_LONGITUDE:
								final double dropOffLongitude = Double.parseDouble(val);
								if ((dropOffLongitude > NYCTLCDimensionalityTypeProvider.MAX_LON)
										|| (dropOffLongitude < NYCTLCDimensionalityTypeProvider.MIN_LON)) {
									success = false;
									break;
								}
								pt.setDropoffLongitude(dropOffLongitude);
								break;
							case DROPOFF_LATITUDE:
								final double dropOffLatitude = Double.parseDouble(val);
								if ((dropOffLatitude > NYCTLCDimensionalityTypeProvider.MAX_LAT)
										|| (dropOffLatitude < NYCTLCDimensionalityTypeProvider.MIN_LAT)) {
									success = false;
									break;
								}
								pt.setDropoffLatitude(dropOffLatitude);
								break;
							case DROPOFF_LOCATION:
								// do nothing
								break;
							case PAYMENT_TYPE:
								final String pmntType = val.toLowerCase().replace(
										"_",
										"").replace(
										" ",
										"");
								if (NumberUtils.isNumber(pmntType)) {
									pt.setPaymentType(Integer.parseInt(val));
								}
								else {
									if (pmntType.contains("crd") || pmntType.contains("cre")) {
										pt.setPaymentType(1);
									}
									else if (pmntType.contains("cas") || pmntType.contains("csh")) {
										pt.setPaymentType(2);
									}
									else if (pmntType.contains("nocharge") || pmntType.equals("no")
											|| pmntType.equals("na") || pmntType.equals("noc")) {
										pt.setPaymentType(3);
									}
									else if (pmntType.contains("dis")) {
										pt.setPaymentType(4);
									}
									else if (pmntType.contains("unk")) {
										pt.setPaymentType(5);
									}
									else if (pmntType.contains("void")) {
										pt.setPaymentType(6);
									}
									else {
										success = false;
										LOGGER.warn("Unknown payment type [" + pmntType + "]");
										pt.setPaymentType(0);
									}
								}
								break;
							case FARE_AMOUNT:
								pt.setFareAmount(Double.parseDouble(val));
								break;
							case EXTRA:
								pt.setExtra(Double.parseDouble(val));
								break;
							case MTA_TAX:
								pt.setMtaTax(Double.parseDouble(val));
								break;
							case IMPROVEMENT_SURCHARGE:
								pt.setImprovementSurcharge(Double.parseDouble(val));
								break;
							case TIP_AMOUNT:
								pt.setTipAmount(Double.parseDouble(val));
								break;
							case TOLLS_AMOUNT:
								pt.setTollsAmount(Double.parseDouble(val));
								break;
							case TOTAL_AMOUNT:
								pt.setTotalAmount(Double.parseDouble(val));
								break;
							case TRIP_TYPE:
								pt.setTripType(Integer.parseInt(val));
								break;
							case EHAIL_FEE:
								pt.setEhailFee(Double.parseDouble(val));
								break;
						}

						if (success) {
							requiredFieldsMissing.remove(field);
						}
					}
					catch (NumberFormatException | NullPointerException e) {
						if (!val.isEmpty()) {
							LOGGER.warn("Unable to parse field [" + field + "] with value [" + val + "]");
						}
						else {
							LOGGER.info("Field [" + field + "] has no value");
						}
					}
				}
				else {
					if (field == null) {
						LOGGER.warn("Unknown field encountered: " + field);
					}
				}
			}
			if (requiredFieldsMissing.isEmpty()) {
				SimpleFeature f = new NYCTLCIngestPlugin().toGeoWaveDataInternalWrapper(
						pt,
						null,
						null);

				if (transformToWebMercator && f != null) {
					try {

						f.setAttribute(
								NYCTLCUtils.Field.PICKUP_LOCATION.getIndexedName(),
								JTS.transform(
										(Geometry) f.getAttribute(NYCTLCUtils.Field.PICKUP_LOCATION.getIndexedName()),
										transform.get()));
						f.setAttribute(
								NYCTLCUtils.Field.DROPOFF_LOCATION.getIndexedName(),
								JTS.transform(
										(Geometry) f.getAttribute(NYCTLCUtils.Field.DROPOFF_LOCATION.getIndexedName()),
										transform.get()));
						// f.setAttribute(index, value);
					}
					catch (MismatchedDimensionException | TransformException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				return f;
			}

			return null;
		}

		@Override
		public SimpleFeature call(
				Row input )
				throws Exception {
			return apply(input);
		}
	}
}
