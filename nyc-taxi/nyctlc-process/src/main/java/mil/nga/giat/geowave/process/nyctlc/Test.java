package mil.nga.giat.geowave.process.nyctlc;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;

import mil.nga.giat.geowave.process.nyctlc.BreakpointColorRamp.Breakpoint;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;

public class Test
{
	public static void main(
			final String[] args )
			throws IOException {
		// final SparkContext sc = new SparkContext();
		// final JavaPairRDD<Long, Double> rdd = JavaPairRDD.fromRDD(
		// sc.objectFile(
		// "s3://nyctlc-data/kde-test_11",
		// sc.defaultMinPartitions(),
		// (ClassTag) scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)),
		// (ClassTag) scala.reflect.ClassTag$.MODULE$.apply(Long.class),
		// (ClassTag) scala.reflect.ClassTag$.MODULE$.apply(Double.class));
		// final List<Tuple2<Long, Double>> list = rdd.collect();
		// for (Tuple2<Long, Double> e : list) {
		// System.err.println("Cell id: " + e._1);
		// System.err.println("Percentile: " + e._2);
		// Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> t
		// = tmsCellToTileAndPosition(
		// e._1,
		// 11);
		//
		// System.err.println("Tile X: " + t._1._2());
		//
		// System.err.println("Tile Y: " + t._1._3());
		// System.err.println("X: " + t._2._1);
		// System.err.println("Y: " + t._2._2);
		// }
		// sc.stop();
		BufferedImage img = new BufferedImage(
				1000,
				20,
				BufferedImage.TYPE_INT_ARGB);
		for (int i = 0; i < 1000; i++) {
			for (int y = 0; y < 20; y++) {
				img.setRGB(
						i,
						y,
						ramp.getColor(
								i / 1000.0).getRGB());
			}
		}
		ImageIO.write(
				img,
				"png",
				new File(
						"C:/Temp/testramp.png"));
	}

	private static final BreakpointColorRamp ramp = getDivergentColorMap();

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

	public static BreakpointColorRamp getColorRamp() {
		final List<Breakpoint> breakpoints = new ArrayList<>();
		Color c1 = hex2Rgb(
				"D10031",
				1f);
		Color c2 = hex2Rgb(
				"D10000",
				1f);
		double[] hsv = ColorUtils.RGBtoHSV(
				c1.getRed(),
				c1.getGreen(),
				c1.getBlue());
		System.err.println(Arrays.toString(hsv));
		hsv = ColorUtils.RGBtoHSV(
				c2.getRed(),
				c2.getGreen(),
				c2.getBlue());
		System.err.println(Arrays.toString(hsv));
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
		// breakpoints.add(
		// new Breakpoint(
		// 0.0,
		// hex2Rgb(
		// "D10031",
		// 1f)));
		// breakpoints.add(
		// new Breakpoint(
		// 1.0,
		// hex2Rgb(
		// "D10000",
		// 1f)));
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
