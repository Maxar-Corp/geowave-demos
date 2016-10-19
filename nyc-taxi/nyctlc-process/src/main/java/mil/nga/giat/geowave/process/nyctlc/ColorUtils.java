package mil.nga.giat.geowave.process.nyctlc;

import java.awt.Color;
import java.util.ArrayList;

public class ColorUtils
{
	/**
	 * Method returns three integers representing the RGB values of a HSV input
	 * Values in input must be between 0 and 1 Values in output are between 0
	 * and 255.
	 * 
	 * See http://www.easyrgb.com/math.php?MATH=M21#text21 for details of
	 * algorithm used.
	 * 
	 * @param hue
	 * @param saturation
	 * @param value
	 * @return
	 */
	public static int[] HSVtoRGB(
			final double hue,
			final double saturation,
			final double value ) {
		int R, G, B;
		// String sR="ff",sG="ff",sB="ff";
		if (saturation == 0) // HSV values = 0 - 1
		{
			R = new Double(
					value * 255).intValue();
			G = new Double(
					value * 255).intValue();
			B = new Double(
					value * 255).intValue();
		}
		else {
			double var_h = hue * 6;
			if (var_h == 6) {
				var_h = 0; // H must be < 1
			}
			final int var_i = new Double(
					Math.floor(var_h)).intValue(); // Or ... var_i = floor(
			// var_h )
			final double var_1 = value * (1 - saturation);
			final double var_2 = value * (1 - saturation * (var_h - var_i));
			final double var_3 = value * (1 - saturation * (1 - (var_h - var_i)));
			double var_r;
			double var_g;
			double var_b;
			if (var_i == 0) {
				var_r = value;
				var_g = var_3;
				var_b = var_1;
			}
			else if (var_i == 1) {
				var_r = var_2;
				var_g = value;
				var_b = var_1;
			}
			else if (var_i == 2) {
				var_r = var_1;
				var_g = value;
				var_b = var_3;
			}
			else if (var_i == 3) {
				var_r = var_1;
				var_g = var_2;
				var_b = value;
			}
			else if (var_i == 4) {
				var_r = var_3;
				var_g = var_1;
				var_b = value;
			}
			else {
				var_r = value;
				var_g = var_1;
				var_b = var_2;
			}
			R = new Double(
					var_r * 255).intValue(); // RGB results = 0 - 255
			G = new Double(
					var_g * 255).intValue();
			B = new Double(
					var_b * 255).intValue();
		}
		final int[] returnArray = new int[3];
		returnArray[0] = R;
		returnArray[1] = G;
		returnArray[2] = B;
		return returnArray;
	}

	/**
	 * Method returns three doubles representing the HSV values of a RGB input
	 * Values in input must be between 0 and 255 Values in output are between 0
	 * and 1.
	 * 
	 * See http://www.easyrgb.com/math.php?MATH=M21#text21 for details of
	 * algorithm used.
	 * 
	 * @param red
	 * @param green
	 * @param blue
	 * @return
	 */
	public static double[] RGBtoHSV(
			final int red,
			final int green,
			final int blue ) {
		double H = 0, S = 0, V = 0;

		final double R = red / 255.0;
		final double G = green / 255.0;
		final double B = blue / 255.0;

		final double var_Min = Math.min(
				Math.min(
						R,
						G),
				B); // Min. value of RGB
		final double var_Max = Math.max(
				Math.max(
						R,
						G),
				B); // Max. value of RGB
		final double del_Max = var_Max - var_Min; // Delta RGB value

		V = var_Max;

		if (del_Max == 0) {
			// This is a gray, no chroma...
			H = 0; // HSV results = 0 - 1
			S = 0;
		}
		else {
			// Chromatic data...
			S = del_Max / var_Max;

			final double del_R = (((var_Max - R) / 6.0) + (del_Max / 2.0)) / del_Max;
			final double del_G = (((var_Max - G) / 6.0) + (del_Max / 2.0)) / del_Max;
			final double del_B = (((var_Max - B) / 6.0) + (del_Max / 2.0)) / del_Max;

			if (R == var_Max) {
				H = del_B - del_G;
			}
			else if (G == var_Max) {
				H = (1.0 / 3.0) + del_R - del_B;
			}
			else if (B == var_Max) {
				H = (2.0 / 3.0) + del_G - del_R;
			}
			if (H < 0) {
				H += 1;
			}
			if (H > 1) {
				H -= 1;
			}
		}

		return new double[] {
			H,
			S,
			V
		};
	}

	public static int RGBtoINT(
			final int r,
			final int g,
			final int b ) {
		final int intVal = (((r & 0xFF) << 16) | ((g & 0xFF) << 8) | ((b & 0xFF) << 0));
		return (0xff000000 | intVal);
	}

	public static int[] INTtoRGB(
			final int rgb ) {
		final int[] returnArray = new int[3];

		final int r = (rgb >> 16) & 0xFF;
		final int g = (rgb >> 8) & 0xFF;
		final int b = (rgb >> 0) & 0xFF;

		returnArray[0] = r;
		returnArray[1] = g;
		returnArray[2] = b;
		return returnArray;
	}

	public static String getHexValue(
			final int r,
			final int g,
			final int b ) {
		return (pad(Integer.toHexString(r)) + pad(Integer.toHexString(g)) + pad(Integer.toHexString(b))).toUpperCase();
	}

	private static String pad(
			final String in ) {
		if (in.length() == 0) {
			return "00";
		}
		if (in.length() == 1) {
			return "0" + in;
		}
		return in;
	}

	static public Color getAlphaedColor(
			final Color c,
			final double alpha ) {
		return new Color(
				c.getRed(),
				c.getGreen(),
				c.getBlue(),
				(int) (255 * alpha));
	}

	static public Color interpolateRGB(
			final Color minColor,
			final Color maxColor,
			final double fraction ) {
		final int red = minColor.getRed() + (int) ((maxColor.getRed() - minColor.getRed()) * fraction + 0.5);
		final int green = minColor.getGreen() + (int) ((maxColor.getGreen() - minColor.getGreen()) * fraction + 0.5);
		final int blue = minColor.getBlue() + (int) ((maxColor.getBlue() - minColor.getBlue()) * fraction + 0.5);
		return new Color(
				red,
				green,
				blue);
	}

	static public Color interpolateRGBA(
			final Color minColor,
			final Color maxColor,
			final double fraction ) {
		final int red = minColor.getRed() + (int) ((maxColor.getRed() - minColor.getRed()) * fraction + 0.5);
		final int green = minColor.getGreen() + (int) ((maxColor.getGreen() - minColor.getGreen()) * fraction + 0.5);
		final int blue = minColor.getBlue() + (int) ((maxColor.getBlue() - minColor.getBlue()) * fraction + 0.5);
		final int alpha = minColor.getAlpha() + (int) ((maxColor.getAlpha() - minColor.getAlpha()) * fraction + 0.5);
		return new Color(
				red,
				green,
				blue,
				alpha);
	}

	static public Color interpolateHSV(
			final Color minColor,
			final Color maxColor,
			final double fraction ) {
		final double minHSV[] = RGBtoHSV(
				minColor.getRed(),
				minColor.getGreen(),
				minColor.getBlue());
		final double maxHSV[] = RGBtoHSV(
				maxColor.getRed(),
				maxColor.getGreen(),
				maxColor.getBlue());
		final double stepH = (maxHSV[0] - minHSV[0]) * fraction;

		final double stepS = (maxHSV[1] - minHSV[1]) * fraction;
		final double stepV = (maxHSV[2] - minHSV[2]) * fraction;
		double newH = stepH + minHSV[0];
		if (newH < 0) {
			newH += 1.0;
		}
		else if (newH > 1) {
			newH -= 1.0;
		}
		final int nextRGB[] = HSVtoRGB(
				newH,
				minHSV[1] + stepS,
				minHSV[2] + stepV);

		return new Color(
				nextRGB[0],
				nextRGB[1],
				nextRGB[2]);
	}

	static public Color inverseInterpolateHSV(
			final Color interpolatedColor,
			final Color otherColor,
			final double fraction ) {
		final double interpolatedHSV[] = RGBtoHSV(
				interpolatedColor.getRed(),
				interpolatedColor.getGreen(),
				interpolatedColor.getBlue());
		final double otherHSV[] = RGBtoHSV(
				otherColor.getRed(),
				otherColor.getGreen(),
				otherColor.getBlue());
		// double myH = (interpolatedHSV[0] - fraction * otherHSV[0]) / (1 -
		// fraction);
		// final double myS = (interpolatedHSV[1] - fraction * otherHSV[1]) / (1
		// - fraction);
		// final double myV = (interpolatedHSV[2] - fraction * otherHSV[2]) / (1
		// - fraction);
		double myH = interpolatedHSV[0] * (1 - fraction) + otherHSV[0] * fraction;
		final double myS = interpolatedHSV[1] * (1 - fraction) + otherHSV[1] * fraction;
		final double myV = interpolatedHSV[2] * (1 - fraction) + otherHSV[2] * fraction;

		if (myH < 0) {
			myH += 1.0;
		}
		else if (myH > 1) {
			myH -= 1.0;
		}
		final int myRGB[] = HSVtoRGB(
				myH,
				myS,
				myV);

		return new Color(
				myRGB[0],
				myRGB[1],
				myRGB[2]);
	}

	static public ArrayList<Color> createGradient(
			final Color minColor,
			final Color maxColor,
			final int count ) {
		final ArrayList<Color> colorList = new ArrayList<Color>(
				count);

		final double minHSV[] = RGBtoHSV(
				minColor.getRed(),
				minColor.getGreen(),
				minColor.getBlue());
		final double maxHSV[] = RGBtoHSV(
				maxColor.getRed(),
				maxColor.getGreen(),
				maxColor.getBlue());

		double stepH;
		if (maxHSV[0] - minHSV[0] > 0.5) {
			stepH = -(minHSV[0] + 1.0 - maxHSV[0]) / (count - 1);
		}
		else if (maxHSV[0] - minHSV[0] < -0.5) {
			stepH = (maxHSV[0] + 1.0 - minHSV[0]) / (count - 1);
		}
		else {
			stepH = (maxHSV[0] - minHSV[0]) / (count - 1);
		}

		final double stepS = (maxHSV[1] - minHSV[1]) / (count - 1);
		final double stepV = (maxHSV[2] - minHSV[2]) / (count - 1);

		double nextH = minHSV[0];
		double nextS = minHSV[1];
		double nextV = minHSV[2];

		for (int i = 0; i < count; i++) {
			final int nextRGB[] = HSVtoRGB(
					nextH,
					nextS,
					nextV);

			final Color nextColor = new Color(
					nextRGB[0],
					nextRGB[1],
					nextRGB[2]);
			colorList.add(nextColor);

			nextH += stepH;
			if (nextH < 0) {
				nextH += 1.0;
			}
			else if (nextH > 1) {
				nextH -= 1.0;
			}

			nextS += stepS;
			nextV += stepV;
		}

		return colorList;
	}

	public static Color getMajorColorOffset(
			final Color original ) {
		Color offsetColor = null;
		boolean darker = original.getRed() + original.getBlue() + original.getGreen() > 382;
		if (getDeltaColor(original) > 200) {
			darker = !darker;
		}
		if (darker) {
			if (original.equals(Color.white)) {
				offsetColor = Color.lightGray;
			}
			else {
				offsetColor = original.darker().darker();
			}
		}
		else {
			if (original.equals(Color.black)) {
				offsetColor = Color.darkGray;
			}
			else {
				offsetColor = original.brighter().brighter();
			}
		}
		return offsetColor;
	}

	public static Color getMajorValueOffset(
			final Color original ) {
		final double[] hsv = RGBtoHSV(
				original.getRed(),
				original.getGreen(),
				original.getBlue());
		if (hsv[2] < 0.2) {
			hsv[2] += 0.6;
		}
		else if (hsv[2] > 0.6) {
			hsv[2] -= 0.4;
		}
		else {
			hsv[2] += 0.4;
		}
		final int rgb[] = HSVtoRGB(
				hsv[0],
				hsv[1],
				hsv[2]);
		return new Color(
				rgb[0],
				rgb[1],
				rgb[2],
				original.getAlpha());

	}

	public static Color getColorRotation(
			final Color original ) {
		return new Color(
				original.getGreen(),
				original.getBlue(),
				255 - original.getRed(),
				original.getAlpha());

	}

	public static Color getMinorValueOffset(
			final Color original ) {
		final double[] hsv = RGBtoHSV(
				original.getRed(),
				original.getGreen(),
				original.getBlue());
		if (hsv[2] < 0.2) {
			hsv[2] += 0.3;
		}
		else if (hsv[2] > 0.6) {
			hsv[2] -= 0.2;
		}
		else {
			hsv[2] += 0.2;
		}
		final int rgb[] = HSVtoRGB(
				hsv[0],
				hsv[1],
				hsv[2]);
		return new Color(
				rgb[0],
				rgb[1],
				rgb[2],
				original.getAlpha());

	}

	public static Color getValueOffset(
			final Color original,
			double offset ) {
		final double[] hsv = RGBtoHSV(
				original.getRed(),
				original.getGreen(),
				original.getBlue());
		offset = Math.min(
				Math.max(
						-0.5,
						offset),
				0.5);
		if (hsv[2] > 0.5) {
			hsv[2] -= offset;
		}
		else {
			hsv[2] += offset;
		}
		final int rgb[] = HSVtoRGB(
				hsv[0],
				hsv[1],
				hsv[2]);
		return new Color(
				rgb[0],
				rgb[1],
				rgb[2],
				original.getAlpha());

	}

	public static Color incrementValue(
			final Color original,
			final double increment ) {
		final double[] hsv = RGBtoHSV(
				original.getRed(),
				original.getGreen(),
				original.getBlue());
		hsv[2] += increment;
		hsv[2] = clamp(
				hsv[2],
				0,
				1);
		final int rgb[] = HSVtoRGB(
				hsv[0],
				hsv[1],
				hsv[2]);
		return new Color(
				rgb[0],
				rgb[1],
				rgb[2],
				original.getAlpha());
	}

	protected static double clamp(
			final double x,
			final double min,
			final double max ) {
		if (x < min) {
			return min;
		}
		if (x > max) {
			return max;
		}
		return x;
	}

	public static Color getMinorColorOffset(
			final Color original ) {
		Color offsetColor = null;
		boolean darker = original.getRed() + original.getBlue() + original.getGreen() > 382;
		if (getDeltaColor(original) > 200) {
			darker = !darker;
		}
		if (darker) {
			if (original.equals(Color.white)) {
				offsetColor = Color.lightGray.brighter();
			}
			else {
				offsetColor = original.darker();
			}
		}
		else {
			if (original.equals(Color.black)) {
				offsetColor = Color.darkGray.darker();
			}
			else {
				offsetColor = original.brighter();
			}
		}
		return offsetColor;
	}

	public static int getDeltaColor(
			final Color color ) {
		final int maxC = Math.max(
				Math.max(
						color.getBlue(),
						color.getGreen()),
				color.getRed());
		final int minC = Math.min(
				Math.min(
						color.getBlue(),
						color.getGreen()),
				color.getRed());
		return maxC - minC;
	}

	static public Color getAverageColor(
			final Color before,
			final Color after ) {
		final float hue = (getHue(before) + getHue(after)) / 2f;
		final float saturation = (getSaturation(before) + getSaturation(after)) / 2f;
		final float brightness = (getBrightness(before) + getBrightness(after)) / 2f;

		final Color newColor = new Color(
				Color.HSBtoRGB(
						hue,
						saturation,
						brightness));
		return newColor;
	}

	static public float getHue(
			final Color color ) {
		return Color.RGBtoHSB(
				color.getRed(),
				color.getGreen(),
				color.getBlue(),
				null)[0];
	}

	static public float getSaturation(
			final Color color ) {
		return Color.RGBtoHSB(
				color.getRed(),
				color.getGreen(),
				color.getBlue(),
				null)[1];
	}

	static public float getBrightness(
			final Color color ) {
		return Color.RGBtoHSB(
				color.getRed(),
				color.getGreen(),
				color.getBlue(),
				null)[2];
	}
}
