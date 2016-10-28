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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

public class DiffWeightsToTMS
{
	private static final BreakpointColorRamp ramp = getDivergentColorMap();

	public static void main(
			final String[] args ) {
		try (final JavaSparkContext sc = new JavaSparkContext()) {
			final int minLevel = Integer.parseInt(
					args[0]);

			final int maxLevel = Integer.parseInt(
					args[1]);
			final int minYear = Integer.parseInt(
					args[2]);

			final int maxYear = Integer.parseInt(
					args[3]);
			final String dataType = args[4];
			final String taxiType1 = args[5];
			final String locationType1 = args[6];
			final String[] filters1 = args[7].split(
					",");
			final String[] taxiTypes1 = taxiType1.equals(
					"both") ? new String[] {
						"green",
						"yellow"
			} : new String[] {
				taxiType1
			};
			final String[] locationTypes1 = locationType1.equals(
					"both") ? new String[] {
						"pickup",
						"dropoff"
			} : new String[] {
				locationType1
			};
			final String taxiType2 = args[8];
			final String locationType2 = args[9];
			final String[] filters2 = args[10].split(
					",");
			final String[] taxiTypes2 = taxiType1.equals(
					"both") ? new String[] {
						"green",
						"yellow"
			} : new String[] {
				taxiType2
			};
			final String[] locationTypes2 = locationType1.equals(
					"both") ? new String[] {
						"pickup",
						"dropoff"
			} : new String[] {
				locationType2
			};
			final String rootKDEDir = args[11].endsWith(
					"/") ? args[11] : args[11] + "/";
			final PairFunction<Tuple2<Long, Tuple2<Double, Double>>, Long, Double> calculateDiff = t -> {
				final double diff = t._2._1 - t._2._2;
				return new Tuple2<Long, Double>(
						t._1,
						diff);
			};
			for (int zoom = minLevel; zoom <= maxLevel; zoom++) {

				final JavaPairRDD<Long, Double> rdd1 = WeightsToTMS.getCombinedRdd(
						rootKDEDir,
						taxiTypes1,
						locationTypes1,
						filters1,
						minYear,
						maxYear,
						dataType,
						zoom,
						sc);
				final JavaPairRDD<Long, Double> rdd2 = WeightsToTMS.getCombinedRdd(
						rootKDEDir,
						taxiTypes2,
						locationTypes2,
						filters2,
						minYear,
						maxYear,
						dataType,
						zoom,
						sc);
				final JavaPairRDD<Long, Double> percentileRdd = WeightsToTMS.getPercentileRDD(
						rdd2.join(
								rdd1).mapToPair(
										calculateDiff));
				SparkTMSFromS3InTMS.renderTMS(
						zoom,
						percentileRdd,
						args[12],
						args[13]);
				percentileRdd.unpersist(
						false);
			}
			sc.stop();
		}
	}

	public static JavaPairRDD<Long, Double> getRDD(
			final String rootKDEDir,
			final String taxiType,
			final int year,
			final int zoom,
			final String locationType,
			final String filter,
			final String dataType,
			final JavaSparkContext sc ) {
		final String objPath = rootKDEDir + year + "/" + taxiType + "/" + filter + "/" + locationType + "/" + zoom;
		final JavaPairRDD<Long, NYCTLCData> pairRDD = JavaPairRDD.fromJavaRDD(sc.objectFile(objPath));
		return getRDDByDataType(
				pairRDD,
				dataType);
	}

	private static JavaPairRDD<Long, Double> getRDDByDataType(
			final JavaPairRDD<Long, NYCTLCData> pairRDD,
			final String dataType ) {
		final PairFunction<Tuple2<Long, NYCTLCData>, Long, Double> toDataType = i -> new Tuple2<>(
				i._1,
				i._2.getValue(
						dataType));
		return pairRDD.mapToPair(
				toDataType);
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

	public static BreakpointColorRamp getDivergentColorMap() {

		final List<Breakpoint> breakpoints = new ArrayList<>();
		breakpoints.add(new Breakpoint(
				0.0,
				new Color(
						0.2298057f,
						0.298717966f,
						0.753683153f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				0.03125,
				new Color(
						0.26623388f,
						0.353094838f,
						0.801466763f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				0.0625,
				new Color(
						0.30386891f,
						0.406535296f,
						0.84495867f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				0.09375,
				new Color(
						0.342804478f,
						0.458757618f,
						0.883725899f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				0.125,
				new Color(
						0.38301334f,
						0.50941904f,
						0.917387822f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				0.15625,
				new Color(
						0.424369608f,
						0.558148092f,
						0.945619588f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				0.1875,
				new Color(
						0.46666708f,
						0.604562568f,
						0.968154911f,
						1.0f)));

		breakpoints.add(new Breakpoint(
				0.21875,
				new Color(
						0.509635204f,
						0.648280772f,
						0.98478814f,
						0.9f)));

		breakpoints.add(new Breakpoint(
				0.25,
				new Color(
						0.552953156f,
						0.688929332f,
						0.995375608f,
						0.8f)));

		breakpoints.add(new Breakpoint(
				0.28125,
				new Color(
						0.596262162f,
						0.726149107f,
						0.999836203f,
						0.7f)));

		breakpoints.add(new Breakpoint(
				0.3125,
				new Color(
						0.639176211f,
						0.759599947f,
						0.998151185f,
						0.5f)));

		breakpoints.add(new Breakpoint(
				0.34375,
				new Color(
						0.681291281f,
						0.788964712f,
						0.990363227f,
						0.25f)));

		breakpoints.add(new Breakpoint(
				0.375,
				new Color(
						0.722193294f,
						0.813952739f,
						0.976574709f,
						0.0f)));

		breakpoints.add(new Breakpoint(
				0.40625,
				new Color(
						0.761464949f,
						0.834302879f,
						0.956945269f,
						0.0f)));
		breakpoints.add(new Breakpoint(
				0.4375,
				new Color(
						0.798691636f,
						0.849786142f,
						0.931688648f,
						0.0f)));
		breakpoints.add(new Breakpoint(
				0.46875,
				new Color(
						0.833466556f,
						0.860207984f,
						0.901068838f,
						0.0f)));
		breakpoints.add(new Breakpoint(
				0.5,
				new Color(
						0.865395197f,
						0.86541021f,
						0.865395561f,
						0.0f)));
		breakpoints.add(new Breakpoint(
				0.53125,
				new Color(
						0.897787179f,
						0.848937047f,
						0.820880546f,
						0.0f)));
		breakpoints.add(new Breakpoint(
				0.5625,
				new Color(
						0.924127593f,
						0.827384882f,
						0.774508472f,
						0.0f)));
		breakpoints.add(new Breakpoint(
				0.59375,
				new Color(
						0.944468518f,
						0.800927443f,
						0.726736146f,
						0.0f)));
		breakpoints.add(new Breakpoint(
				0.625,
				new Color(
						0.958852946f,
						0.769767752f,
						0.678007945f,
						0.0f)));
		breakpoints.add(new Breakpoint(
				0.65625,
				new Color(
						0.96732803f,
						0.734132809f,
						0.628751763f,
						0.25f)));
		breakpoints.add(new Breakpoint(
				0.6875,
				new Color(
						0.969954137f,
						0.694266682f,
						0.579375448f,
						0.5f)));
		breakpoints.add(new Breakpoint(
				0.71875,
				new Color(
						0.966811177f,
						0.650421156f,
						0.530263762f,
						0.7f)));
		breakpoints.add(new Breakpoint(
				0.75,
				new Color(
						0.958003065f,
						0.602842431f,
						0.481775914f,
						0.8f)));
		breakpoints.add(new Breakpoint(
				0.78125,
				new Color(
						0.943660866f,
						0.551750968f,
						0.434243684f,
						0.9f)));
		breakpoints.add(new Breakpoint(
				0.8125,
				new Color(
						0.923944917f,
						0.49730856f,
						0.387970225f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				0.84375,
				new Color(
						0.89904617f,
						0.439559467f,
						0.343229596f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				0.875,
				new Color(
						0.869186849f,
						0.378313092f,
						0.300267182f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				0.90625,
				new Color(
						0.834620542f,
						0.312874446f,
						0.259301199f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				0.9375,
				new Color(
						0.795631745f,
						0.24128379f,
						0.220525627f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				0.96875,
				new Color(
						0.752534934f,
						0.157246067f,
						0.184115123f,
						1.0f)));
		breakpoints.add(new Breakpoint(
				1.0,
				new Color(
						0.705673158f,
						0.01555616f,
						0.150232812f,
						1.0f)));
		return new BreakpointColorRamp(
				breakpoints);
	}

	public static BreakpointColorRamp getColorRamp() {
		final List<Breakpoint> breakpoints = new ArrayList<>();
		breakpoints.add(new Breakpoint(
				0.0,
				new Color(
						0f,
						0f,
						0f,
						0.0f)));
		breakpoints.add(new Breakpoint(
				0.2,
				new Color(
						0f,
						0f,
						0f,
						0.0f)));
		breakpoints.add(new Breakpoint(
				0.3,
				hex2Rgb(
						"000052",
						0.1f)));
		breakpoints.add(new Breakpoint(
				0.4,
				hex2Rgb(
						"000075",
						0.2f)));
		breakpoints.add(new Breakpoint(
				0.5,
				hex2Rgb(
						"380099",
						0.3f)));
		breakpoints.add(new Breakpoint(
				0.6,
				hex2Rgb(
						"5700AD",
						0.4f)));
		breakpoints.add(new Breakpoint(
				0.7,
				hex2Rgb(
						"7500BD",
						0.5f)));
		breakpoints.add(new Breakpoint(
				0.8,
				hex2Rgb(
						"9A00BD",
						0.6f)));
		breakpoints.add(new Breakpoint(
				0.85,
				hex2Rgb(
						"BD00BA",
						0.7f)));
		breakpoints.add(new Breakpoint(
				0.9,
				hex2Rgb(
						"C20085",
						0.8f)));
		breakpoints.add(new Breakpoint(
				0.92,
				hex2Rgb(
						"C40062",
						0.9f)));
		breakpoints.add(new Breakpoint(
				0.93,
				hex2Rgb(
						"D1004D",
						0.95f)));
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
