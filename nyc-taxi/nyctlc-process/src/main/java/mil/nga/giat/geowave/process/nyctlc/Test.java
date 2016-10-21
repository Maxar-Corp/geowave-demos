package mil.nga.giat.geowave.process.nyctlc;

import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;

public class Test
{
	public static void main(
			final String[] args ) {
		final SparkContext sc = new SparkContext();
		final JavaPairRDD<Long, Double> rdd = JavaPairRDD.fromRDD(
				sc.objectFile(
						"s3://nyctlc-data/kde-test_11",
						sc.defaultMinPartitions(),
						(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)),
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(Long.class),
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(Double.class));
		final List<Tuple2<Long, Double>> list = rdd.collect();
		for (Tuple2<Long, Double> e : list) {
			System.err.println("Cell id: " + e._1);
			System.err.println("Percentile: " + e._2);
			Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> t = tmsCellToTileAndPosition(
					e._1,
					11);

			System.err.println("Tile X: " + t._1._2());

			System.err.println("Tile Y: " + t._1._3());
			System.err.println("X: " + t._2._1);
			System.err.println("Y: " + t._2._2);
		}
		sc.stop();
	}

	public static Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> tmsCellToTileAndPosition(
			final long cellIndex,
			final int zoom ) {
		final int numPosts = 1 << zoom;
		final int xPost = (int) (cellIndex / numPosts);
		final int yPost = (int) (cellIndex % numPosts);

		return new Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>(
				new Tuple3<Integer, Integer, Integer>(
						zoom,
						xPost / 256,
						yPost / 256),
				new Tuple2<Integer, Integer>(
						xPost % 256,
						yPost % 256));
	}
}
