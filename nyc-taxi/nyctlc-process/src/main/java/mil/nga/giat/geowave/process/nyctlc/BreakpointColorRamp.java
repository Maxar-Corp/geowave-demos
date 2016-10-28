package mil.nga.giat.geowave.process.nyctlc;

import java.awt.Color;
import java.util.ArrayList;
import java.util.List;

public class BreakpointColorRamp
{
	public static class Breakpoint
	{
		private Double value;
		private Color color;

		public Breakpoint(
				final Double value,
				final Color color ) {
			this.value = value;
			this.color = color;
		}

		public Double getValue() {
			return value;
		}

		public Color getColor() {
			return color;
		}

		public void setValue(
				final Double value ) {
			this.value = value;
		}

		public void setColor(
				final Color color ) {
			this.color = color;
		}
	}

	final private Breakpoint[] breakpointColors;

	public BreakpointColorRamp(
			final List<Breakpoint> breakpointColors ) {
		this.breakpointColors = breakpointColors.toArray(new Breakpoint[] {});
	}

	// do a full deep copy so that we have no ties to the original breakpoints
	public List<Breakpoint> copyBreakPoints() {
		if (breakpointColors == null) {
			return null;
		}

		final List<Breakpoint> deepCopy = new ArrayList<Breakpoint>();
		for (final Breakpoint b : breakpointColors) {
			final Color c = b.getColor();

			final Breakpoint bkptCopy = new Breakpoint(
					new Double(
							b.getValue()),
					new Color(
							c.getRed(),
							c.getGreen(),
							c.getBlue(),
							c.getAlpha()));

			deepCopy.add(bkptCopy);
		}

		return deepCopy;
	}

	/**
	 *
	 * @param value
	 *            value in the range of breakpoints
	 * @return The color represented at that gradient level
	 */
	public Color getColor(
			final double value ) {
		return getBreakpoint(
				value).getColor();
	}

	/**
	 * returns the interpolated color and index for the given a value
	 *
	 * @param value
	 *            The value to look up the coloring for
	 * @return The interpolated color and index
	 */
	private Breakpoint getBreakpoint(
			final double value ) {
		int minColorIndex = -1;
		for (final Breakpoint bpVal : breakpointColors) {
			if (bpVal.getValue() > value) {
				break;
			}
			minColorIndex++;
		}
		if (minColorIndex > 0) {
			if (minColorIndex >= (breakpointColors.length - 1)) {
				return new Breakpoint(
						new Double(
								minColorIndex),
						breakpointColors[minColorIndex].getColor());
			}
			else {
				return getBreakpoint(
						minColorIndex,
						value);
			}
		}
		else {
			if (breakpointColors.length == 0) {
				return new Breakpoint(
						0.0,
						Color.black);
			}
			else {
				if (breakpointColors.length < 2) {
					return new Breakpoint(
							0.0,
							breakpointColors[0].getColor());
				}
				return getBreakpoint(
						0,
						value);
			}
		}
	}

	public static void main(
			String[] args ) {

	}

	private Breakpoint getBreakpoint(
			final int minColorIndex,
			final double value ) {
		final Breakpoint minBP = breakpointColors[minColorIndex];
		final Breakpoint maxBP = breakpointColors[minColorIndex + 1];
		final Double minValue = minBP.getValue();
		final Double maxValue = maxBP.getValue();

		final double fraction = (value - minValue) / (maxValue - minValue);
		if (fraction < 0) {
			return new Breakpoint(
					new Double(
							minColorIndex),
					new Color(
							0,
							0,
							0,
							0));
		}
		if (fraction > 1) {
			return new Breakpoint(
					new Double(
							minColorIndex + 1),
					new Color(
							0,
							0,
							0,
							0));
		}
		final double alpha = ((minBP.getColor().getAlpha() * (1.0 - fraction)) + (maxBP.getColor().getAlpha() * fraction)) / 255.0;

		return new Breakpoint(
				minColorIndex + fraction,
				ColorUtils.getAlphaedColor(
						ColorUtils.interpolateRGB(
								minBP.getColor(),
								maxBP.getColor(),
								fraction),
						alpha));
	}

	public double getValue(
			final int index ) {
		return breakpointColors[index].getValue();
	}

	public Color getColor(
			final int index ) {
		return breakpointColors[index].getColor();
	}

	public int getColorCount() {
		return breakpointColors.length;
	}

	public double getIndex(
			final double value ) {
		return getBreakpoint(
				value).getValue();
	}

	public void setValue(
			final int index,
			final double value ) {
		breakpointColors[index].setValue(value);
	}

	public void setColor(
			final int index,
			final Color color ) {
		breakpointColors[index].setColor(color);
	}
}
