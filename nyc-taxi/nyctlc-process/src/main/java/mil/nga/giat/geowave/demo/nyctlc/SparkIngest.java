package mil.nga.giat.geowave.demo.nyctlc;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.rdd.OrderedRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.mortbay.log.Log;
import org.opengis.feature.simple.SimpleFeature;
import org.spark_project.guava.collect.Ordering;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
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
	private final static Logger LOGGER = Logger.getLogger(
			SparkIngest.class);

	public static void main(
			final String[] args ) {
		final SparkSession spark = SparkSession
				.builder()
				.master(
						"local")
				.config(
						"spark.serializer",
						"org.apache.spark.serializer.KryoSerializer")
				.appName(
						"my app")
				.config(
						"spark.sql.warehouse.dir",
						"file:///C:/development/projects/geowave/code/geowave-demos/nyc-taxi/nyctlc-process/spark-warehouse")
				.getOrCreate();
		int year = 2012;
		final List<String> paths = new ArrayList<>();
		for (int month = 8; month <= 12; month++) {
			paths.add(
					String.format(
							"s3://nyc-tlc/trip+data/green_tripdata_%04d-%02d.csv",
							year,
							month));
		}
		for (year = 2013; year <= 2015; year++) {
			for (int month = 1; month <= 12; month++) {
				paths.add(
						String.format(
								"s3://nyc-tlc/trip+data/green_tripdata_%04d-%02d.csv",
								year,
								month));
			}
		}
		year = 2016;
		for (int month = 1; month <= 6; month++) {
			paths.add(
					String.format(
							"s3://nyc-tlc/trip+data/green_tripdata_%04d-%02d.csv",
							year,
							month));
		}
		final Dataset<Row> df = spark
				.read()
				.format(
						"csv")
				.option(
						"header",
						"true")
				.option(
						"nullValue",
						"")
				.csv(
						// paths.toArray(
						// new String[] {}));
						"C:\\Users\\rfecher\\Downloads\\green_tripdata_2013-08.csv");
		
		RDD<Tuple2<Key, Value>> rdd = df.mapPartitions(
				new IngestFunction2(),
				Encoders.tuple(
						Encoders.kryo(
								Key.class),
						Encoders.kryo(
								Value.class)))
		.rdd();
		JavaPairRDD javaRdd = JavaPairRDD.fromRDD(rdd,  (ClassTag)scala.reflect.ClassTag$.MODULE$.apply(
				Key.class),  (ClassTag)scala.reflect.ClassTag$.MODULE$.apply(
						Value.class));
		Configuration conf = new Configuration();
		javaRdd.sortByKey().saveAsNewAPIHadoopFile(
				"C:\\Temp\\tmp.rfile",
				Key.class,
				Value.class,
				AccumuloFileOutputFormat.class,
				conf);
		// Job job = Job.getInstance(spark.conf())
		// df.show();
		// df.foreachPartition(
		// new IngestFunction());
//		(Ordering<Key>)Ordering.from(new Comparator<Key>() {
//
//			@Override
//			public int compare(
//					Key o1,
//					Key o2 ) {
//				return 0;
//			}})
		// Job j = Job.getInstance(conf);
		// j.setOutputFormatClass(AccumuloFileOutputFormat.class);
//		OrderedRDDFunctions orderedRdd = RDD.rddToOrderedRDDFunctions(
//				df
//						.mapPartitions(
//								new IngestFunction2(),
//								Encoders.tuple(
//										Encoders.kryo(
//												Key.class),
//										Encoders.kryo(
//												Value.class)))
//						.rdd(), scala.math.Ordering.Implicits$.,(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(
//								Key.class),
//						(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(
//								Value.class));
//		orderedRdd.sortByKey(true, orderedRdd.sortByKey$default$2());
//		RDD.rddToPairRDDFunctions(orderedRdd.org$apache$spark$rdd$OrderedRDDFunctions$$self,
//				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(
//						Key.class),
//				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(
//						Value.class),
//				null).saveAsNewAPIHadoopFile(
//						"C:\\Temp\\tmp.rfile",
//						Key.class,
//						Value.class,
//						AccumuloFileOutputFormat.class,
//						conf);
		// val job = Job.getInstance(df..);
		// instance.setAccumuloConfig(job)
		// val conf = job.getConfiguration
		// val outPath = HdfsUtils.tmpPath(ingestPath, UUID.randomUUID.toString,
		// conf)
		// val failuresPath = outPath.suffix("-failures")
		//
		// HdfsUtils.ensurePathExists(failuresPath, conf)
		// kvPairs
		// .sortByKey()
		// .saveAsNewAPIHadoopFile(
		// outPath.toString,
		// classOf[Key],
		// classOf[Value],
		// classOf[AccumuloFileOutputFormat],
		// conf)
		//
		// val ops = instance.connector.tableOperations()
		// ops.importDirectory(table, outPath.toString, failuresPath.toString,
		// true)
		// String tableName = args[0];
		//
		// SparkConf sparkConf = new SparkConf().setAppName(
		// "JavaHBaseBulkPutExample " + tableName);
		//
		// Configuration conf = HBaseConfiguration.create();
		//
		// JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
		//
		// hbaseContext.bulkPut(rdd,
		// TableName.valueOf(tableName),
		// new PutFunction());
		// JavaSparkContext jsc = new JavaSparkContext(
		// sparkConf);
	}

	// public write(kvPairs: RDD[(Key, Value)], instance: AccumuloInstance,
	// table: String): Unit = {
	// val sc = kvPairs.sparkContext
	// val job = Job.getInstance(sc.hadoopConfiguration)
	// instance.setAccumuloConfig(job)
	// val conf = job.getConfiguration
	// val outPath = HdfsUtils.tmpPath(ingestPath, UUID.randomUUID.toString,
	// conf)
	// val failuresPath = outPath.suffix("-failures")
	//
	// HdfsUtils.ensurePathExists(failuresPath, conf)
	// kvPairs
	// .sortByKey()
	// .saveAsNewAPIHadoopFile(
	// outPath.toString,
	// classOf[Key],
	// classOf[Value],
	// classOf[AccumuloFileOutputFormat],
	// conf)
	//
	// val ops = instance.connector.tableOperations()
	// ops.importDirectory(table, outPath.toString, failuresPath.toString, true)
	//
	// // cleanup ingest directories on success
	// val fs = ingestPath.getFileSystem(conf)
	// if( fs.exists(new Path(outPath, "_SUCCESS")) ) {
	// fs.delete(outPath, true)
	// fs.delete(failuresPath, true)
	// } else {
	// throw new java.io.IOException(s"Accumulo bulk ingest failed at
	// $ingestPath")
	// }
	// }
	public static class IngestFunction implements
			ForeachPartitionFunction<Row>
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void call(
				final Iterator<Row> row )
				throws Exception {
			final WritableDataAdapter a = new NYCTLCIngestPlugin().getDataAdapters(
					"")[0];
			final AccumuloKeyValuePairGenerator<SimpleFeature> generator = new AccumuloKeyValuePairGenerator<SimpleFeature>(
					a,
					new SpatialDimensionalityTypeProvider().createPrimaryIndex(),
					null);
			// row.forEachRemaining(
			// () -> {
			// generator.constructKeyValuePairs(a.getAdapterId(),)
			// }
		}

	}

	public static class IngestFunction2 implements
			MapPartitionsFunction<Row, scala.Tuple2<Key, Value>>
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<scala.Tuple2<Key, Value>> call(
				final Iterator<Row> arg0 )
				throws Exception {

			final WritableDataAdapter a = new NYCTLCIngestPlugin().getDataAdapters(
					"")[0];
			final AccumuloKeyValuePairGenerator<SimpleFeature> generator = new AccumuloKeyValuePairGenerator<SimpleFeature>(
					a,
					new SpatialDimensionalityTypeProvider().createPrimaryIndex(),
					DataStoreUtils.UNCONSTRAINED_VISIBILITY);
			return Iterators.filter(
					Iterators.transform(
							arg0,
							new RowToKV(
									generator,
									a.getAdapterId().getBytes())),
					new Predicate() {

						@Override
						public boolean apply(
								Object input ) {
							return input != null;
						}
					});

		}

	}

	public static class RowToKV implements
			Function<Row, scala.Tuple2<Key, Value>>
	{
		AccumuloKeyValuePairGenerator<SimpleFeature> generator;
		private final RowToFeature rToF = new RowToFeature();
		private final byte[] adapterId;

		public RowToKV(
				final AccumuloKeyValuePairGenerator<SimpleFeature> generator,
				final byte[] adapterId ) {
			this.generator = generator;
			this.adapterId = adapterId;
		}

		@Override
		public scala.Tuple2<Key, Value> apply(
				final Row input ) {
			final SimpleFeature f = rToF.apply(
					input);
			if (f == null) {
				return null;
			}
			final List<AccumuloKeyValuePair> pairs = generator.constructKeyValuePairs(
					adapterId,
					f);
			if (!pairs.isEmpty()) {
				AccumuloKeyValuePair pair = pairs.get(
						0);
				return new scala.Tuple2<Key, Value>(
						pair.getKey(),
						pair.getValue());
			}
			return null;

		}
	}

	public static class RowToFeature implements
			Function<Row, SimpleFeature>
	{
		private final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
		private final static String DATE_START_FORMAT = "yyyyMMdd";

		private final static SimpleDateFormat dateFormat = new SimpleDateFormat(
				DATE_FORMAT);
		private final static SimpleDateFormat dateStartFormat = new SimpleDateFormat(
				DATE_START_FORMAT);

		@Override
		public SimpleFeature apply(
				final Row input ) {
			final NYCTLCEntry pt = new NYCTLCEntry();

			pt.setCabType(
					2);
			// Sets the cabType beforehand because there is no cabType
			// column in the table
			final Set<Field> requiredFieldsMissing = new HashSet<Field>(
					NYCTLCUtils.REQUIRED_FIELDS);
			for (int fieldIdx = 0; fieldIdx < input.length(); fieldIdx++) {
				final String val = input.getString(
						fieldIdx);
				final NYCTLCUtils.Field field = NYCTLCUtils.Field.getField(
						input.schema().fieldNames()[fieldIdx]);
				boolean success = true;
				if ((field != null) && (val != null)) {
					try {
						switch (field) {
							case VENDOR_ID:
								if (val.equals(
										"1")
										|| val.equals(
												"2")) {
									pt.setVendorId(
											Integer.parseInt(
													val));
								}
								else {
									if (val.toLowerCase().equals(
											"cmt")) {
										pt.setVendorId(
												1);
										requiredFieldsMissing.remove(
												field);
									}
									else if (val.toLowerCase().equals(
											"vts")) {
										pt.setVendorId(
												2);
										requiredFieldsMissing.remove(
												field);
									}
									else {
										success = false;
										pt.setVendorId(
												0); // unknown
									}
								}
								break;
							case PICKUP_DATETIME:
								try {
									final Date pickupDate = dateFormat.parse(
											val);
									pt.setPickupDatetime(
											pickupDate.getTime());

									final Date pickupDateStart = dateStartFormat.parse(
											dateStartFormat.format(
													pickupDate));
									pt.setTimeOfDaySec(
											new Long(
													TimeUnit.MILLISECONDS.toSeconds(
															pickupDate.getTime() - pickupDateStart.getTime()))
																	.longValue());

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
									pt.setDropoffDatetime(
											dateFormat.parse(
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
								pt.setPassengerCount(
										Integer.parseInt(
												val));
								break;
							case TRIP_DISTANCE:
								pt.setTripDistance(
										Double.parseDouble(
												val));
								break;
							case PICKUP_LONGITUDE:
								final double longitude = Double.parseDouble(
										val);
								if ((longitude > NYCTLCDimensionalityTypeProvider.MAX_LON)
										|| (longitude < NYCTLCDimensionalityTypeProvider.MIN_LON)) {
									success = false;
									break;
								}
								pt.setPickupLongitude(
										longitude);
								break;
							case PICKUP_LATITUDE:
								final double latitude = Double.parseDouble(
										val);
								if ((latitude > NYCTLCDimensionalityTypeProvider.MAX_LAT)
										|| (latitude < NYCTLCDimensionalityTypeProvider.MIN_LAT)) {
									success = false;
									break;
								}
								pt.setPickupLatitude(
										latitude);
								break;
							case PICKUP_LOCATION:
								// do nothing
								break;
							case STORE_AND_FWD_FLAG:
								final String strFwdFlg = val.toLowerCase();
								pt.setStoreAndFwdFlag(
										strFwdFlg.equals(
												"1")
												|| strFwdFlg.equals(
														"y"));
								break;
							case RATE_CODE_ID:
								pt.setRateCodeId(
										Integer.parseInt(
												val));
								break;
							case DROPOFF_LONGITUDE:
								final double dropOffLongitude = Double.parseDouble(
										val);
								if ((dropOffLongitude > NYCTLCDimensionalityTypeProvider.MAX_LON)
										|| (dropOffLongitude < NYCTLCDimensionalityTypeProvider.MIN_LON)) {
									success = false;
									break;
								}
								pt.setDropoffLongitude(
										dropOffLongitude);
								break;
							case DROPOFF_LATITUDE:
								final double dropOffLatitude = Double.parseDouble(
										val);
								if ((dropOffLatitude > NYCTLCDimensionalityTypeProvider.MAX_LAT)
										|| (dropOffLatitude < NYCTLCDimensionalityTypeProvider.MIN_LAT)) {
									success = false;
									break;
								}
								pt.setDropoffLatitude(
										dropOffLatitude);
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
								if (NumberUtils.isNumber(
										pmntType)) {
									pt.setPaymentType(
											Integer.parseInt(
													val));
								}
								else {
									if (pmntType.contains(
											"crd")
											|| pmntType.contains(
													"cre")) {
										pt.setPaymentType(
												1);
									}
									else if (pmntType.contains(
											"cas")
											|| pmntType.contains(
													"csh")) {
										pt.setPaymentType(
												2);
									}
									else if (pmntType.contains(
											"nocharge")
											|| pmntType.equals(
													"no")
											|| pmntType.equals(
													"na")
											|| pmntType.equals(
													"noc")) {
										pt.setPaymentType(
												3);
									}
									else if (pmntType.contains(
											"dis")) {
										pt.setPaymentType(
												4);
									}
									else if (pmntType.contains(
											"unk")) {
										pt.setPaymentType(
												5);
									}
									else if (pmntType.contains(
											"void")) {
										pt.setPaymentType(
												6);
									}
									else {
										success = false;
										LOGGER.warn(
												"Unknown payment type [" + pmntType + "]");
										pt.setPaymentType(
												0);
									}
								}
								break;
							case FARE_AMOUNT:
								pt.setFareAmount(
										Double.parseDouble(
												val));
								break;
							case EXTRA:
								pt.setExtra(
										Double.parseDouble(
												val));
								break;
							case MTA_TAX:
								pt.setMtaTax(
										Double.parseDouble(
												val));
								break;
							case IMPROVEMENT_SURCHARGE:
								pt.setImprovementSurcharge(
										Double.parseDouble(
												val));
								break;
							case TIP_AMOUNT:
								pt.setTipAmount(
										Double.parseDouble(
												val));
								break;
							case TOLLS_AMOUNT:
								pt.setTollsAmount(
										Double.parseDouble(
												val));
								break;
							case TOTAL_AMOUNT:
								pt.setTotalAmount(
										Double.parseDouble(
												val));
								break;
							case TRIP_TYPE:
								pt.setTripType(
										Integer.parseInt(
												val));
								break;
							case EHAIL_FEE:
								pt.setEhailFee(
										Double.parseDouble(
												val));
								break;
						}

						if (success) {
							requiredFieldsMissing.remove(
									field);
						}
					}
					catch (NumberFormatException | NullPointerException e) {
						if (!val.isEmpty()) {
							LOGGER.warn(
									"Unable to parse field [" + field + "] with value [" + val + "]");
						}
						else {
							LOGGER.info(
									"Field [" + field + "] has no value");
						}
					}
				}
				else {
					if (field == null) {
						LOGGER.warn(
								"Unknown field encountered: " + field);
					}
				}
			}
			if (requiredFieldsMissing.isEmpty()) {
				return new NYCTLCIngestPlugin().toGeoWaveDataInternalWrapper(
						pt,
						null,
						null);
			}

			return null;
		}
	}

}
