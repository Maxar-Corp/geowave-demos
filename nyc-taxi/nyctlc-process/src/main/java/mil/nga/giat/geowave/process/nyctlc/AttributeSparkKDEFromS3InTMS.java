package mil.nga.giat.geowave.process.nyctlc;

import java.util.ArrayList;
import java.util.List;

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

public class AttributeSparkKDEFromS3InTMS
{
	public static void main(
			final String[] args )
			throws NoSuchAuthorityCodeException,
			FactoryException {
		final SparkSession spark = SparkSession
				.builder()
				.appName(
						"nyctlc kde")
				.config(
						"spark.serializer",
						"org.apache.spark.serializer.KryoSerializer")
				.config(
						"spark.kryo.registrator",
						GeoWaveRegistrator.class.getCanonicalName())
//				.config(
//						"spark.sql.warehouse.dir",
//						"file:///C:/Temp/spark-warehouse")
//				.master(
//						"local")
				.getOrCreate();
		int year = 2013;
		final List<String> paths = new ArrayList<>();
//		for (int month = 8; month <= 10; month++) {
//			paths.add(
//					String.format(
//							"s3://nyc-tlc/trip data/green_tripdata_%04d-%02d.csv",
//							year,
//							month));
//		}
//		for (year = 2014; year <= 2015; year++) {
//			for (int month = 1; month <= 12; month++) {
//				paths.add(
//						String.format(
//								"s3://nyc-tlc/trip data/green_tripdata_%04d-%02d.csv",
//								year,
//								month));
//			}
//		}
//		year = 2016;
//		for (int month = 1; month <= 6; month++) {
//			paths.add(
//					String.format(
//							"s3://nyc-tlc/trip data/green_tripdata_%04d-%02d.csv",
//							year,
//							month));
//		}
//		for (year = 2009; year <= 2015; year++) {
//			for (int month = 1; month <= 12; month++) {
//				paths.add(
//						String.format(
//								"s3://nyc-tlc/trip data/yellow_tripdata_%04d-%02d.csv",
//								year,
//								month));
//			}
//		}
		year = 2016;
		for (int month = 1; month <= 6; month++) {
			paths.add(
					String.format(
							"s3://nyc-tlc/trip data/yellow_tripdata_%04d-%02d.csv",
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
						 paths.toArray(
						 new String[] {}));

//						"C:\\Users\\rfecher\\Downloads\\green_tripdata_2013-08.csv");
		final int minLevel = Integer.parseInt(
				args[0]);

		final int maxLevel = Integer.parseInt(
				args[1]);
		String cqlFilterStr = null;
		if (args.length > 4) {
			cqlFilterStr = args[4];
		}
		JavaRDD<SimpleFeature> sfRdd = df.toJavaRDD().map(
				new RowToFeature(
						false));

		if ((cqlFilterStr != null) && !cqlFilterStr.isEmpty()) {
			sfRdd = sfRdd.filter(
					new CQLFilterFunction(
							cqlFilterStr));
		}
		final TDigestSerializable td = sfRdd.sample(
				false,
				0.2).aggregate(
						new TDigestSerializable(),
						new Function2<TDigestSerializable, SimpleFeature, TDigestSerializable>() {

							/**
							 *
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public TDigestSerializable call(
									final TDigestSerializable v2,
									final SimpleFeature v1 )
									throws Exception {
								if (v1 != null) {
									final Object o = v1.getAttribute(
											args[3]);
									if ((o != null) && (o instanceof Number)) {
										v2.tdigest.add(
												((Number) o).doubleValue());
									}
								}
								return v2;
							}
						},
						new Function2<TDigestSerializable, TDigestSerializable, TDigestSerializable>() {

							/**
							 *
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public TDigestSerializable call(
									final TDigestSerializable v1,
									final TDigestSerializable v2 )
									throws Exception {
								v1.tdigest.add(
										v2.tdigest);
								return v1;
							}
						});
		final PairFlatMapFunction<SimpleFeature, Tuple2<Integer, Long>, Tuple2<Double, Double>> cellMapper = s -> SparkKDE
				.getTMSCellsWithAttribute(
						s,
						false,
						minLevel,
						maxLevel,
						args[3],
						td);
		final PairFunction<Tuple2<Long, Tuple2<Double, Double>>, Double, Long> avgAttribute = i -> new Tuple2<>(
				i._2._2,
				i._1);

		final Function<Tuple2<Double, Double>, Tuple2<Double, Double>> identity = x -> x;

		final Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>> sum = (
				final Tuple2<Double, Double> x,
				final Tuple2<Double, Double> y ) -> {
			return new Tuple2<Double, Double>(
					x._1 + y._1,
					x._2 + y._2);
		};
		final JavaPairRDD<Tuple2<Integer, Long>, Tuple2<Double, Double>> pairRDD = sfRdd.flatMapToPair(
				cellMapper).combineByKey(
						identity,
						sum,
						sum);
		for (int zoom = minLevel; zoom <= maxLevel; zoom++) {
			final int finalZoom = zoom;
			final JavaPairRDD<Long, Tuple2<Double, Double>> levelRdd = getRDDByLevel(
					pairRDD,
					zoom);

			final JavaPairRDD<Double, Long> sampledPercentiles = pairRDD
					.mapToPair(
							new PairFunction<Tuple2<Tuple2<Integer, Long>, Tuple2<Double, Double>>, Double, Long>() {

								/**
								 *
								 */
								private static final long serialVersionUID = 1L;

								@Override
								public Tuple2<Double, Long> call(
										final Tuple2<Tuple2<Integer, Long>, Tuple2<Double, Double>> t )
										throws Exception {
									return new Tuple2<Double, Long>(
											t._2._1,
											t._1._2);
								}

							})
					.sample(
							false,
							0.2)
					.sortByKey();
			final PairFunction<Tuple2<Tuple2<Double, Long>, Long>, Long, Double> swap = i -> new Tuple2<>(
					i._2,
					i._1._1);
			final JavaPairRDD<Long, Double> rankedPercentiles = sampledPercentiles.zipWithIndex().mapToPair(
					swap);
			final long rankedPercentileCount = rankedPercentiles.cache().count() - 1;

			final List<Double> doubles = rankedPercentiles.lookup(
					(long) Math.floor(
							rankedPercentileCount * 0.1));
			final double filterWeight = doubles.get(
					0);
			rankedPercentiles.unpersist();

			final Function<Tuple2<Long, Tuple2<Double, Double>>, Boolean> filter = i -> i._2._1 > filterWeight;
			final JavaPairRDD<Double, Long> rdd = levelRdd
					.filter(
							filter)
					.mapToPair(
							avgAttribute)
					.sortByKey();
			final long count = rdd.cache().count() - 1;
			final PairFunction<Tuple2<Tuple2<Double, Long>, Long>, Long, Double> calculatePercentile = t -> {
				final double percentile = (double) t._2 / (double) count;
				return new Tuple2<Long, Double>(
						t._1._2,
						percentile);
			};

//			final JavaPairRDD<Long, Double> newRdd = 
					rdd
					.zipWithIndex()
					.mapToPair(
							calculatePercentile)
					.repartition(
							1)
					.saveAsObjectFile(
							args[2] + "_" + finalZoom);

			// newRdd.saveAsTextFile("C:\\Temp\\rdd.txt");
			// SparkTMSFromS3InTMS.renderTMS(
			// zoom,
			// newRdd,
			// "");
			sfRdd.unpersist(
					true);

			rdd.unpersist(
					false);
		}
		spark.stop();
	}

	private static JavaPairRDD<Long, Tuple2<Double, Double>> getRDDByLevel(
			final JavaPairRDD<Tuple2<Integer, Long>, Tuple2<Double, Double>> pairRDD,
			final Integer level ) {
		final PairFunction<Tuple2<Tuple2<Integer, Long>, Tuple2<Double, Double>>, Long, Tuple2<Double, Double>> removeLevel = i -> new Tuple2<>(
				i._1._2,
				i._2);
		return pairRDD
				.filter(
						v -> v._1._1.equals(
								level))
				.mapToPair(
						removeLevel);
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

}
