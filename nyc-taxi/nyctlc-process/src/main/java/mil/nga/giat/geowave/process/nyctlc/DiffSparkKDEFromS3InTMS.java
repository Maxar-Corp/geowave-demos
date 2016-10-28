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

public class DiffSparkKDEFromS3InTMS
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
//				 .config(
//				 "spark.sql.warehouse.dir",
//				 "file:///C:/Temp/spark-warehouse")
//				 .master(
//				 "local")
				.getOrCreate();
		int year = 2013;
		final List<String> paths = new ArrayList<>();
//		for (int month = 8; month <= 12; month++) {
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
		for (int month = 6; month <= 6; month++) {
//			for (int month = 1; month <= 6; month++) {
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

//		 "C:\\Users\\rfecher\\Downloads\\green_tripdata_2013-08.csv");
		final int minLevel = Integer.parseInt(
				args[0]);

		final int maxLevel = Integer.parseInt(
				args[1]);
		String cqlFilterStr = null;
		if (args.length > 3) {
			cqlFilterStr = args[3];
		}
		JavaRDD<SimpleFeature> sfRdd = df.toJavaRDD().map(
				new RowToFeature(
						false));

		if ((cqlFilterStr != null) && !cqlFilterStr.isEmpty()) {
			sfRdd = sfRdd.filter(
					new CQLFilterFunction(
							cqlFilterStr));
		}
		final Function<Double, Double> identity = x -> x;

		final Function2<Double, Double, Double> sum = (
				final Double x,
				final Double y ) -> {
			return x + y;
		};
		final PairFlatMapFunction<SimpleFeature, Tuple2<Integer, Long>, Double> pickupCellMapper = s -> SparkKDE
				.getTMSCells(
						s,
						false,
						minLevel,
						maxLevel);
		final PairFlatMapFunction<SimpleFeature, Tuple2<Integer, Long>, Double> dropOfffCellMapper = s -> SparkKDE
				.getTMSCells(
						s,
						true,
						minLevel,
						maxLevel);
		final JavaPairRDD<Tuple2<Integer, Long>, Double> rddPickups = sfRdd
				.flatMapToPair(
						pickupCellMapper)
				.combineByKey(
						identity,
						sum,
						sum)
				.cache();
		final JavaPairRDD<Tuple2<Integer, Long>, Double> rddDropOffs = sfRdd
				.flatMapToPair(
						dropOfffCellMapper)
				.combineByKey(
						identity,
						sum,
						sum)
				.cache();
		for (int zoom = minLevel; zoom <= maxLevel; zoom++) {
			final int finalZoom = zoom;
			final JavaPairRDD<Long,Double> levelPickupRdd = getRDDByLevel(
					rddPickups,
					zoom);
			final JavaPairRDD<Long,Double> levelDropoffRdd = getRDDByLevel(
					rddDropOffs,
					zoom);

//			final long pickupCount = levelPickupRdd.cache().count() - 1;
//			final PairFunction<Tuple2<Tuple2<Double, Long>, Long>, Long, Double> calculatePercentilePickup = t -> {
//				final double percentile = (double) t._2 / (double) pickupCount;
//				return new Tuple2<Long, Double>(
//						t._1._2,
//						percentile);
//			};
//			final JavaPairRDD<Long, Double> finalRddPickups = levelPickupRdd.sortByKey().zipWithIndex().mapToPair(
//					calculatePercentilePickup);
//			final long dropoffCount = levelDropoffRdd.cache().count() - 1;
//			final PairFunction<Tuple2<Tuple2<Double, Long>, Long>, Long, Double> calculatePercentileDropoff = t -> {
//				final double percentile = (double) t._2 / (double) dropoffCount;
//				return new Tuple2<Long, Double>(
//						t._1._2,
//						percentile);
//			};
//			final JavaPairRDD<Long, Double> finalRddDropOffs = levelDropoffRdd.sortByKey().zipWithIndex().mapToPair(
//					calculatePercentileDropoff);
			final PairFunction<Tuple2<Long, Tuple2<Double, Double>>, Double, Long> calculateDiff = t -> {
				final double ratio = t._2._1 - t._2._2;
				return new Tuple2<Double, Long>(
						ratio,
						t._1);
			};
			final JavaPairRDD<Double, Long> ratioRdd = levelDropoffRdd
					.join(
							levelPickupRdd)
					.mapToPair(
							calculateDiff)
					.sortByKey();

			final long ratioCount = ratioRdd.cache().count() - 1;
			final PairFunction<Tuple2<Tuple2<Double, Long>, Long>, Long, Double> calculatePercentileRatio = t -> {
				final double percentile = (double) t._2 / (double) ratioCount;
				return new Tuple2<Long, Double>(
						t._1._2,
						percentile);
			};
//			 SparkTMSFromS3InTMS.renderTMS(zoom,
			ratioRdd
					.zipWithIndex()
					.mapToPair(
							calculatePercentileRatio)
					.repartition(
							1)
					.saveAsObjectFile(
							args[2] + "_" + finalZoom);
//					,"");
			// newRdd.saveAsTextFile("C:\\Temp\\rdd_" + zoom);
//			 SparkTMSFromS3InTMS.renderTMS(
//			 zoom,
//			 newRdd,
//			 "");
		}
		spark.stop();

	}

	private static JavaPairRDD<Long,Double> getRDDByLevel(
			final JavaPairRDD<Tuple2<Integer, Long>, Double> pairRDD,
			final Integer level ) {
		final PairFunction<Tuple2<Tuple2<Integer, Long>, Double>, Long, Double> swap = i -> new Tuple2<>(
				i._1._2,
				i._2);
		return pairRDD
				.filter(
						v -> v._1._1.equals(
								level))
				.mapToPair(
						swap);
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
