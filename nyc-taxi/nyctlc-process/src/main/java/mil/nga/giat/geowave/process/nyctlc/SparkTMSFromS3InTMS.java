package mil.nga.giat.geowave.process.nyctlc;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import mil.nga.giat.geowave.process.nyctlc.BreakpointColorRamp.Breakpoint;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;

public class SparkTMSFromS3InTMS
{
	private static final BreakpointColorRamp ramp = getColorRamp();

	public static void main(
			final String[] args ) {
		final SparkContext sc = new SparkContext();
		final int minLevel = Integer.parseInt(args[0]);

		final int maxLevel = Integer.parseInt(args[1]);
		for (int zoom = minLevel; zoom <= maxLevel; zoom++) {
			final JavaPairRDD<Long, Double> rdd = JavaPairRDD.fromRDD(
					sc.objectFile(
							args[2] + "_" + zoom,
							sc.defaultMinPartitions(),
							(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)),
					(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(Long.class),
					(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(Double.class));

			renderTMS(
					zoom,
					rdd,
					args[3]);
		}
	}

	public static void renderTMS(
			final int zoom,
			final JavaPairRDD<Long, Double> rdd,
			final String outputS3Prefix ) {
		final PairFunction<Tuple2<Long, Double>, Tuple3<Integer, Integer, Integer>, Tuple2<Long, Double>> keyWithTile = (
				final Tuple2<Long, Double> t ) -> {
			final Tuple3<Integer, Integer, Integer> tms = SparkKDE.tmsCellToTile(
					t._1,
					zoom);
			return new Tuple2(
					tms,
					t);
		};
		final Function2<Double[][], Tuple2<Long, Double>, Double[][]> applyValueToMatrix = (
				final Double[][] matrix,
				final Tuple2<Long, Double> t ) -> {
			final Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> tmsAndPosition = SparkKDE
					.tmsCellToTileAndPosition(
							t._1,
							zoom);
			matrix[tmsAndPosition._2._1][tmsAndPosition._2._2] = t._2;
			return matrix;
		};

		final Function<Tuple2<Long, Double>, Double[][]> initializeMatrix = (
				final Tuple2<Long, Double> t ) -> {
			final Double[][] matrix = new Double[SparkKDE.TILE_SIZE][SparkKDE.TILE_SIZE];
			applyValueToMatrix.call(
					matrix,
					t);
			return matrix;
		};
		final Function2<Double[][], Double[][], Double[][]> mergeMatrices = (
				final Double[][] matrix1,
				final Double[][] matrix2 ) -> {
			for (int x = 0; x < matrix2.length; x++) {
				if (matrix2[x] != null) {
					for (int y = 0; y < matrix2.length; y++) {
						if ((matrix2[x][y] != null) && (matrix2[x][y] > 0)) {
							matrix1[x][y] = matrix2[x][y];
						}
					}
				}
			}
			return matrix1;
		};
		rdd
				.mapToPair(
						keyWithTile)
				.combineByKey(
						initializeMatrix,
						applyValueToMatrix,
						mergeMatrices)
				.foreach(
						new WriteTileToS3(
								outputS3Prefix.endsWith(
										"/") ? outputS3Prefix : outputS3Prefix + "/"));
	}

	private static class WriteTileToS3 implements
			VoidFunction<Tuple2<Tuple3<Integer, Integer, Integer>, Double[][]>>
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1L;
		private final String tileBase;
		private static AmazonS3Client s3 = new AmazonS3Client();

		public WriteTileToS3(
				final String tileBase ) {
			this.tileBase = tileBase;
		}

		@Override
		public void call(
				final Tuple2<Tuple3<Integer, Integer, Integer>, Double[][]> t )
				throws Exception {
			final Double[][] matrix = t._2;
			final BufferedImage image = new BufferedImage(
					SparkKDE.TILE_SIZE,
					SparkKDE.TILE_SIZE,
					BufferedImage.TYPE_INT_ARGB);
			for (int x = 0; x < matrix.length; x++) {
				if (matrix[x] != null) {
					for (int y = 0; y < matrix.length; y++) {
						if ((matrix[x][y] != null) && (matrix[x][y] > 0)) {
							image.setRGB(
									x,
									y,
									ramp.getColor(
											matrix[x][y]).getRGB());
						}
					}
				}
			}
			writeImage(
					image,
					getTileURL(t._1));
		}

		private void writeImage(
				final BufferedImage image,
				// final Tuple2<Tuple3<Integer, Integer, Integer>, Double[][]> t
				// ) {
				final String url ) {
			try {
				// File f = new File("C:\\Temp\\tiles\\"+ t._1._1() + "\\" +
				// t._1._2() + "\\" + t._1._3() + ".png");
				// f.getParentFile().mkdirs();
				// ImageIO.write(
				// image,
				// "png",
				// new File("C:\\Temp\\tiles\\"+ t._1._1() + "\\" + t._1._2() +
				// "\\" + t._1._3() + ".png"));

				final ByteArrayOutputStream os = new ByteArrayOutputStream();
				ImageIO.write(
						image,
						"png",
						os);
				final byte[] buffer = os.toByteArray();
				final InputStream is = new ByteArrayInputStream(
						buffer);
				final ObjectMetadata meta = new ObjectMetadata();
				meta.setContentLength(buffer.length);
				meta.setContentType("image/png");
				final AmazonS3URI uri = new AmazonS3URI(
						url);
				final PutObjectRequest putRequest = new PutObjectRequest(
						uri.getBucket(),
						uri.getKey(),
						is,
						meta);
				putRequest.setCannedAcl(CannedAccessControlList.PublicRead);
				s3.putObject(putRequest);
			}
			catch (final IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		private String getTileURL(
				final Tuple3<Integer, Integer, Integer> t ) {
			return (tileBase + t._1() + "/" + t._2() + "/" + t._3()) + ".png";
		}
	}

	public static BreakpointColorRamp getColorRamp() {
		final List<Breakpoint> breakpoints = new ArrayList<>();
		breakpoints.add(new Breakpoint(
				0.0,
				new Color(
						0f,
						0f,
						0f,
						0.6f)));
		breakpoints.add(new Breakpoint(
				0.1,
				hex2Rgb(
						"000052",
						0.75f)));
		breakpoints.add(new Breakpoint(
				0.3,
				hex2Rgb(
						"000075",
						0.8f)));
		breakpoints.add(new Breakpoint(
				0.5,
				hex2Rgb(
						"380099",
						0.9f)));
		breakpoints.add(new Breakpoint(
				0.6,
				hex2Rgb(
						"5700AD",
						0.95f)));
		breakpoints.add(new Breakpoint(
				0.7,
				hex2Rgb(
						"7500BD",
						1f)));
		breakpoints.add(new Breakpoint(
				0.8,
				hex2Rgb(
						"9A00BD",
						1f)));
		breakpoints.add(new Breakpoint(
				0.85,
				hex2Rgb(
						"BD00BA",
						1f)));
		breakpoints.add(new Breakpoint(
				0.9,
				hex2Rgb(
						"C20085",
						1f)));
		breakpoints.add(new Breakpoint(
				0.92,
				hex2Rgb(
						"C40062",
						1f)));
		breakpoints.add(new Breakpoint(
				0.93,
				hex2Rgb(
						"D1004D",
						1f)));
		breakpoints.add(new Breakpoint(
				0.94,
				hex2Rgb(
						"D10031",
						1f)));
		breakpoints.add(new Breakpoint(
				0.95,
				hex2Rgb(
						"D10000",
						1f)));
		breakpoints.add(new Breakpoint(
				0.955,
				hex2Rgb(
						"E60F00",
						1f)));
		breakpoints.add(new Breakpoint(
				0.96,
				hex2Rgb(
						"FF4400",
						1f)));
		breakpoints.add(new Breakpoint(
				0.965,
				hex2Rgb(
						"FF1B1B",
						1f)));
		breakpoints.add(new Breakpoint(
				0.97,
				hex2Rgb(
						"F75220",
						1f)));
		breakpoints.add(new Breakpoint(
				0.975,
				hex2Rgb(
						"FF8112",
						1f)));
		breakpoints.add(new Breakpoint(
				0.98,
				hex2Rgb(
						"FF9A2D",
						1f)));
		breakpoints.add(new Breakpoint(
				0.985,
				hex2Rgb(
						"FFD54A",
						1f)));
		breakpoints.add(new Breakpoint(
				0.99,
				hex2Rgb(
						"FFFF68",
						1f)));
		breakpoints.add(new Breakpoint(
				0.995,
				hex2Rgb(
						"F7FC94",
						1f)));
		breakpoints.add(new Breakpoint(
				0.9995,
				hex2Rgb(
						"FFFFC9",
						1f)));
		breakpoints.add(new Breakpoint(
				1.0,
				new Color(
						1f,
						1f,
						1f,
						1f)));

		return new BreakpointColorRamp(
				breakpoints);
	}

	/**
	 *
	 * @param colorStr
	 *            e.g. "FFFFFF"
	 * @return
	 */
	public static Color hex2Rgb(
			final String colorStr,
			final float opacity ) {
		return new Color(
				Integer.valueOf(
						colorStr.substring(
								0,
								2),
						16) / 256f,
				Integer.valueOf(
						colorStr.substring(
								2,
								4),
						16) / 256f,
				Integer.valueOf(
						colorStr.substring(
								4,
								6),
						16) / 256f,
				opacity);
	}
}
