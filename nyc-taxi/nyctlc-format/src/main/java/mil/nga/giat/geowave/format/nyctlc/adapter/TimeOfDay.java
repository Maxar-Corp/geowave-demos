package mil.nga.giat.geowave.format.nyctlc.adapter;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericValue;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

public class TimeOfDay implements
		Persistable,
		CommonIndexValue
{
	private int timeSeconds;
	private byte[] visibility;

	protected TimeOfDay() {}

	public int getTimeSeconds() {
		return timeSeconds;
	}

	public void setTimeSeconds(
			int timeSeconds ) {
		this.timeSeconds = timeSeconds;
	}

	public TimeOfDay(
			final int timeSeconds,
			final byte[] visibility ) {
		this.timeSeconds = timeSeconds;
		this.visibility = visibility;
	}

	@Override
	public byte[] getVisibility() {
		return visibility;
	}

	@Override
	public void setVisibility(
			final byte[] visibility ) {
		this.visibility = visibility;
	}

	public NumericData toNumericData() {
		return new NumericValue(
				timeSeconds);
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer bytes = ByteBuffer.allocate(4);
		bytes.putInt(timeSeconds);
		return bytes.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		timeSeconds = buf.getInt();
	}

	@Override
	public boolean overlaps(
			final NumericDimensionField[] field,
			final NumericData[] rangeData ) {
		return (int) Math.floor(rangeData[0].getMin()) <= this.timeSeconds
				&& (int) Math.ceil(rangeData[0].getMax()) >= this.timeSeconds;
	}

}