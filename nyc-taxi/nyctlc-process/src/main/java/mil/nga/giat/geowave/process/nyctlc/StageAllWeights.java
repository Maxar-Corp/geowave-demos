package mil.nga.giat.geowave.process.nyctlc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;

import mil.nga.giat.geowave.process.nyctlc.SparkIngest.RowToFeature;
import scala.Tuple2;
import scala.Tuple3;

public class StageAllWeights
{

	public static void main(
			final String[] args )
			throws NoSuchAuthorityCodeException,
			FactoryException {
		final SparkSession spark = SparkSession.builder().appName(
				"nyctlc kde").config(
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
		final int minYear = Integer.parseInt(args[2]);

		final int maxYear = Integer.parseInt(args[3]);
		final String taxiType = args[4];
		final String[] taxiTypes = taxiType.equals("both") ? new String[] {
			"green",
			"yellow"
		} : new String[] {
			taxiType
		};
		final Map<String, List<String>> pathMap = new HashMap<String, List<String>>();
		for (final String taxi : taxiTypes) {
			for (int year = minYear; year <= maxYear; year++) {
				final List<String> paths = new ArrayList<>();
				if ((year < 2013) && "green".equals(taxi)) {
					continue;
				}
				final int minMonth;
				final int maxMonth;
				if (year == 2016) {
					minMonth = 1;
					maxMonth = 6;
				}
				else if ((year == 2013) && "green".equals(taxi)) {
					minMonth = 8;
					maxMonth = 12;
				}
				else {
					minMonth = 1;
					maxMonth = 12;
				}
				for (int month = minMonth; month <= maxMonth; month++) {
					paths.add(String.format(
							"s3://nyc-tlc/trip data/" + taxi + "_tripdata_%04d-%02d.csv",
							year,
							month));
				}
				pathMap.put(
						year + "/" + taxi,
						paths);
			}
		}
		final int minLevel = Integer.parseInt(args[0]);

		final int maxLevel = Integer.parseInt(args[1]);
		final String cqlFilterStr;
		if (args.length > 6) {
			cqlFilterStr = args[6];
		}
		else {
			cqlFilterStr = null;
		}
		final Map<String, String> filters = new HashMap<String, String>();
		filters.put(
				"all",
				"include");
		final Map<String, String> timeOfDayFilters = new HashMap<String, String>();
		timeOfDayFilters.put(
				"morning",
				"(time_of_day_sec > 12600 AND time_of_day_sec <= 34200)");
		timeOfDayFilters.put(
				"midday",
				"(time_of_day_sec > 34200 AND time_of_day_sec <= 55800)");
		timeOfDayFilters.put(
				"evening",
				"(time_of_day_sec > 55800 AND time_of_day_sec <= 77400)");
		timeOfDayFilters.put(
				"late",
				"(time_of_day_sec > 77400 OR time_of_day_sec < 12600)");
		filters.putAll(timeOfDayFilters);
		final Map<String, String> dayOfWeekFilters = new HashMap<String, String>();
		dayOfWeekFilters.put(
				"weekday",
				"(day_of_week > 1 AND day_of_week < 7)");
		dayOfWeekFilters.put(
				"weekend",
				"(day_of_week < 2 OR day_of_week > 6)");
		filters.putAll(dayOfWeekFilters);
		for (final Entry<String, String> e1 : timeOfDayFilters.entrySet()) {
			for (final Entry<String, String> e2 : dayOfWeekFilters.entrySet()) {
				filters.put(
						e2.getKey() + "_" + e1.getKey(),
						e1.getValue() + " AND " + e2.getValue());
			}
		}
		// final ExecutorService executor = Executors.newFixedThreadPool(50);
		for (final Entry<String, List<String>> pathEntry : pathMap.entrySet()) {
			// executor.execute(new Runnable() {
			// @Override
			// public void run() {
			workDataset(
					pathEntry.getKey(),
					pathEntry.getValue(),
					spark,
					minLevel,
					maxLevel,
					cqlFilterStr,
					filters,
					args[5]);
			// }
			// });
		}
		// executor.shutdown();
		// try {
		// executor.awaitTermination(
		// 7,
		// TimeUnit.DAYS);
		// }
		// catch (final InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		spark.stop();
	}

	private static void workDataset(
			final String name,
			final List<String> paths,
			final SparkSession spark,
			final int minZoom,
			final int maxZoom,
			final String cqlFilterStr,
			final Map<String, String> filters,
			final String rootKDEDir ) {
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
						paths.toArray(
								new String[] {}));
		// "C:\\Users\\rfecher\\Downloads\\green_tripdata_2013-08.csv");
		JavaRDD<SimpleFeature> sfRdd = df.toJavaRDD().map(
				new RowToFeature(
						false));

		if ((cqlFilterStr != null) && !cqlFilterStr.isEmpty()) {
			sfRdd = sfRdd.filter(
					new CQLFilterFunction(
							cqlFilterStr));
		}
		final Map<String, TDigestSerializable[]> initialTds = new HashMap<>();
		for (final String filterName : filters.keySet()) {
			final TDigestSerializable[] td = new TDigestSerializable[13];
			for (int i = 0; i < 13; i++) {
				td[i] = new TDigestSerializable();
			}
			initialTds.put(
					filterName,
					td);
		}

		final Map<String, TDigestSerializable[]> resultingTds = sfRdd.aggregate(
				initialTds,
				new TDigestAggregator(
						filters),
				(
						x,
						y ) -> {
					x.forEach(
							(
									k,
									v ) -> {
								final TDigestSerializable[] tds = y.get(
										k);
								for (int i = 0; i < tds.length; i++) {
									tds[i].tdigest.add(
											v[i].tdigest);
								}
							});
					return y;
				});
		final Function<NYCTLCData, NYCTLCData> identity = x -> x;

		final Function2<NYCTLCData, NYCTLCData, NYCTLCData> sum = (
				final NYCTLCData x,
				final NYCTLCData y ) -> {
			return x.add(
					y);
		};
		final JavaPairRDD<Tuple3<Integer, String, Long>, NYCTLCData> pairRDD = sfRdd
				.flatMapToPair(
						new WeightCalculator(
								filters,
								resultingTds,
								minZoom,
								maxZoom))
				.combineByKey(
						identity,
						sum,
						sum)
				.cache();
		final String path = rootKDEDir.endsWith(
				"/") ? rootKDEDir + name + "/" : rootKDEDir + "/" + name + "/";
		for (int zoom = minZoom; zoom <= maxZoom; zoom++) {
			final JavaPairRDD<Tuple2<Long, String>, NYCTLCData> levelRdd = getRDDByLevel(
					pairRDD,
					zoom);
			levelRdd.repartition(
					1).saveAsObjectFile(
							path + "aggregatesByZoom" + "/" + zoom);

			for (final String key : filters.keySet()) {
				final String pickupName = key + "_pickup";
				final String dropoffName = key + "_dropoff";
				final JavaPairRDD<Long, NYCTLCData> pickupRdd = getRDDByName(
						levelRdd,
						pickupName);
				pickupRdd.repartition(
						1).saveAsObjectFile(
								path + key + "/" + "pickup" + "/" + zoom);
				pickupRdd.unpersist(
						false);

				final JavaPairRDD<Long, NYCTLCData> dropoffRdd = getRDDByName(
						levelRdd,
						dropoffName);
				dropoffRdd.repartition(
						1).saveAsObjectFile(
								path + key + "/" + "dropoff" + "/" + zoom);
				dropoffRdd.unpersist(
						false);
			}
			levelRdd.unpersist(
					false);
		}
		sfRdd.unpersist(
				false);
		pairRDD.unpersist(
				false);
	}

	private static JavaPairRDD<Tuple2<Long, String>, NYCTLCData> getRDDByLevel(
			final JavaPairRDD<Tuple3<Integer, String, Long>, NYCTLCData> pairRDD,
			final Integer level ) {
		final PairFunction<Tuple2<Tuple3<Integer, String, Long>, NYCTLCData>, Tuple2<Long, String>, NYCTLCData> removeLevel = i -> new Tuple2<>(
				new Tuple2<>(
						i._1._3(),
						i._1._2()),
				i._2);
		return pairRDD
				.filter(
						v -> v._1._1().equals(
								level))
				.mapToPair(
						removeLevel);
	}

	private static JavaPairRDD<Long, NYCTLCData> getRDDByName(
			final JavaPairRDD<Tuple2<Long, String>, NYCTLCData> pairRDD,
			final String name ) {
		final PairFunction<Tuple2<Tuple2<Long, String>, NYCTLCData>, Long, NYCTLCData> removeName = i -> new Tuple2<>(
				i._1._1,
				i._2);
		return pairRDD
				.filter(
						v -> v._1._2.equals(
								name))
				.mapToPair(
						removeName);
	}

	private static class CQLFilterFunction implements
			Function<SimpleFeature, Boolean>
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 1L;
		private final String cqlFilterStr;
		private Filter filter = null;

		public CQLFilterFunction(
				final String cqlFilterStr ) {
			super();
			this.cqlFilterStr = cqlFilterStr;
		}

		private Filter getFilter() {
			if (filter == null) {
				try {
					filter = CQL.toFilter(cqlFilterStr);
				}
				catch (final CQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return filter;
		}

		@Override
		public Boolean call(
				final SimpleFeature value )
				throws Exception {
			return getFilter().evaluate(
					value);
		}

	}

	private static class TDigestAggregator implements
			Function2<Map<String, TDigestSerializable[]>, SimpleFeature, Map<String, TDigestSerializable[]>>
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1L;
		private final Map<String, String> filterStrings;
		private Map<String, Filter> filters = null;

		public TDigestAggregator(
				final Map<String, String> filterStrings ) {
			this.filterStrings = filterStrings;
		}

		private Map<String, Filter> getFilters() {
			if (filters == null) {
				final Map<String, Filter> internalFilters = new HashMap<>();
				for (final Entry<String, String> entry : filterStrings.entrySet()) {
					try {
						final Filter filter = CQL.toFilter(entry.getValue());
						internalFilters.put(
								entry.getKey(),
								filter);
					}
					catch (final CQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				filters = internalFilters;
			}
			return filters;
		}

		@Override
		public Map<String, TDigestSerializable[]> call(
				final Map<String, TDigestSerializable[]> tds,
				final SimpleFeature f )
				throws Exception {
			if (f != null) {
				final Map<String, Filter> internalFilters = getFilters();
				for (final Entry<String, Filter> filter : internalFilters.entrySet()) {
					if (filter.getValue().evaluate(
							f)) {
						final Number[] values = NYCTLCData.featureToValues(f);
						final TDigestSerializable[] t = tds.get(filter.getKey());
						for (int i = 0; i < t.length; i++) {
							t[i].tdigest.add(values[i].doubleValue());
						}
					}
				}
			}
			return tds;
		}

	}

	private static class WeightCalculator implements
			PairFlatMapFunction<SimpleFeature, Tuple3<Integer, String, Long>, NYCTLCData>
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1L;
		private final Map<String, String> filterStrings;
		final Map<String, TDigestSerializable[]> tds;
		private Tuple2<String, Tuple2<Filter, TDigestSerializable[]>>[] filters = null;
		private final int minZoom;
		private final int maxZoom;

		public WeightCalculator(
				final Map<String, String> filterStrings,
				final Map<String, TDigestSerializable[]> tds,
				final int minZoom,
				final int maxZoom ) {
			this.filterStrings = filterStrings;
			this.tds = tds;
			this.minZoom = minZoom;
			this.maxZoom = maxZoom;

		}

		private Tuple2<String, Tuple2<Filter, TDigestSerializable[]>>[] getFilters() {
			try {
				if (filters == null) {
					final Tuple2<String, Tuple2<Filter, TDigestSerializable[]>>[] internalFilters = new Tuple2[filterStrings
							.size()];
					int i = 0;
					for (final Entry<String, String> entry : filterStrings.entrySet()) {
						Filter filter;
						filter = CQL.toFilter(entry.getValue());

						internalFilters[i++] = new Tuple2<String, Tuple2<Filter, TDigestSerializable[]>>(
								entry.getKey(),
								new Tuple2<Filter, TDigestSerializable[]>(
										filter,
										tds.get(entry.getKey())));
					}
					filters = internalFilters;
				}
			}
			catch (final CQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return filters;
		}

		@Override
		public Iterator<Tuple2<Tuple3<Integer, String, Long>, NYCTLCData>> call(
				final SimpleFeature f )
				throws Exception {
			return SparkKDE.getAllTMSCells(
					f,
					getFilters(),
					minZoom,
					maxZoom);
		}

	}

}
