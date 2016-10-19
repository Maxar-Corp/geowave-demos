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
import org.apache.spark.storage.StorageLevel;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;

import mil.nga.giat.geowave.process.nyctlc.SparkIngest.RowToFeature;
import scala.Tuple2;

public class SparkKDEFromS3
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
				.getOrCreate();
		int year = 2013;
		final List<String> paths = new ArrayList<>();
		for (int month = 8; month <= 12; month++) {
			paths.add(
					String.format(
							"s3://nyc-tlc/trip data/green_tripdata_%04d-%02d.csv",
							year,
							month));
		}
		for (year = 2014; year <= 2015; year++) {
			for (int month = 1; month <= 12; month++) {
				paths.add(
						String.format(
								"s3://nyc-tlc/trip data/green_tripdata_%04d-%02d.csv",
								year,
								month));
			}
		}
		year = 2016;
		for (int month = 1; month <= 6; month++) {
			paths.add(
					String.format(
							"s3://nyc-tlc/trip data/green_tripdata_%04d-%02d.csv",
							year,
							month));
		}
		for (year = 2009; year <= 2015; year++) {
			for (int month = 1; month <= 12; month++) {
				paths.add(
						String.format(
								"s3://nyc-tlc/trip data/yellow_tripdata_%04d-%02d.csv",
								year,
								month));
			}
		}
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
		sfRdd.persist(StorageLevel.MEMORY_AND_DISK());

		for (int level = minLevel; level <= maxLevel; level++) {
			final int numXPosts = (int) Math.pow(
					2,
					level + 1);
			final int numYPosts = (int) Math.pow(
					2,
					level);
			final Function<Double, Double> identity = x -> x;

			final Function2<Double, Double, Double> sum = (
					final Double x,
					final Double y ) -> {
				return x + y;
			};
			final PairFlatMapFunction<SimpleFeature, Long, Double> cellMapper = s -> SparkKDE.getCells(
					s,
					numXPosts,
					numYPosts);
			final PairFunction<Tuple2<Long, Double>, Double, Long> swap = i -> i.swap();
			final JavaPairRDD<Double, Long> rdd = sfRdd
					.flatMapToPair(
							cellMapper)
					.combineByKey(
							identity,
							sum,
							sum)
					.mapToPair(
							swap)
					.sortByKey();

			final long count = rdd.cache().count();
			final PairFunction<Tuple2<Tuple2<Double, Long>, Long>, Long, Double> calculatePercentile = t -> {
				final double percentile = (double) t._2 / (double) count;
				return new Tuple2<Long, Double>(
						t._1._2,
						percentile);
			};
			rdd
					.zipWithIndex()
					.mapToPair(
							calculatePercentile)
					.repartition(
							1)
					.saveAsObjectFile(
							args[2] + "_" + level);
		}
		sfRdd.unpersist(true);
		spark.stop();
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
				catch (CQLException e) {
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
