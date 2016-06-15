package mil.nga.giat.geowave.format.nyctlc.statistics;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils.Field;
import net.sf.json.JSONObject;

import org.opengis.feature.simple.SimpleFeature;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * Created by geowave on 4/28/16.
 */
public class NYCTLCStatistics implements
		Mergeable
{

	private long numEntries = 0;

	private Map<Integer, BaseStat> statsList = new HashMap<Integer, BaseStat>();

	private MinMaxTotStat durationStat = new MinMaxTotStat();

	public NYCTLCStatistics() {}

	public void updateStats(
			final SimpleFeature entry,
			NYCTLCParameters parameters ) {
		numEntries++;

		// update all of our desired stats as defined in the field enum
		final NYCTLCUtils.Field[] fields = NYCTLCUtils.Field.values();
		List<Integer> ordinals = parameters.getOrdinals();
		for (int fieldIdx : ordinals) {
			if (fields[fieldIdx].getStatBuilder() != null) {
				if (entry.getAttribute(fields[fieldIdx].getIndexedName()) != null) {
					BaseStat stat = statsList.get(fieldIdx);
					if (stat == null) {
						stat = fields[fieldIdx].getStatBuilder().build();
						statsList.put(
								fieldIdx,
								stat);
					}
					stat.updateStat(entry.getAttribute(fields[fieldIdx].getIndexedName()));
				}
			}
		}

		if (entry.getAttribute(NYCTLCUtils.Field.DROPOFF_DATETIME.getIndexedName()) != null
				&& entry.getAttribute(NYCTLCUtils.Field.PICKUP_DATETIME.getIndexedName()) != null) {
			Date dropoffDate = (Date) entry.getAttribute(NYCTLCUtils.Field.DROPOFF_DATETIME.getIndexedName());
			Date pickupDate = (Date) entry.getAttribute(NYCTLCUtils.Field.PICKUP_DATETIME.getIndexedName());
			durationStat.updateStat(TimeUnit.MILLISECONDS.toSeconds(dropoffDate.getTime())
					- TimeUnit.MILLISECONDS.toSeconds(pickupDate.getTime()));
		}
	}

	public MinMaxTotStat getDurationStat() {
		return durationStat;
	}

	@Override
	public void merge(
			Mergeable merge ) {
		if ((merge != null) && (merge instanceof NYCTLCStatistics)) {
			NYCTLCStatistics stats = (NYCTLCStatistics) merge;
			numEntries += stats.numEntries;
			for (Entry<Integer, BaseStat> fieldIdx : statsList.entrySet()) {
				fieldIdx.getValue().merge(
						stats.statsList.get(fieldIdx.getKey()));
			}

			durationStat.merge(stats.durationStat);
		}
	}

	@Override
	public byte[] toBinary() {

		byte[][] statsBytes = new byte[statsList.size() + 1][];
		int[] statsOrdinals = new int[statsList.size()];
		int byteLen = 0;
		int statIdx = 0;
		for (Entry<Integer, BaseStat> fieldIdx : statsList.entrySet()) {
			statsOrdinals[statIdx] = fieldIdx.getKey();
			statsBytes[statIdx] = fieldIdx.getValue().toBinary();
			byteLen += statsBytes[statIdx].length + 8;
			statIdx++;
		}
		statsBytes[statsList.size()] = durationStat.toBinary();
		byteLen += statsBytes[statsList.size()].length + 4;

		final ByteBuffer buffer = ByteBuffer.allocate(12 + byteLen);
		buffer.putInt(statsList.size());
		buffer.putLong(numEntries);
		for (int i = 0; i < statsList.size() + 1; i++) {
			if (i < statsOrdinals.length) {
				buffer.putInt(statsOrdinals[i]);
			}
			buffer.putInt(statsBytes[i].length);
			buffer.put(statsBytes[i]);
		}

		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		int statsSize = buffer.getInt();
		numEntries = buffer.getLong();
		statsList.clear();
		Field[] fields = Field.values();
		for (int i = 0; i < statsSize; i++) {
			int ordinal = buffer.getInt();
			int statLen = buffer.getInt();
			BaseStat stat = fields[ordinal].getStatBuilder().build();
			byte[] statBytes = new byte[statLen];
			buffer.get(statBytes);
			stat.fromBinary(statBytes);
			statsList.put(
					ordinal,
					stat);
		}
		int statLen = buffer.getInt();
		byte[] statBytes = new byte[statLen];
		buffer.get(statBytes);

		durationStat = new MinMaxTotStat();
		durationStat.fromBinary(statBytes);
	}

	public JSONObject toJSONObject() {
		final JSONObject statsJson = new JSONObject();

		statsJson.put(
				"count",
				numEntries);

		final NYCTLCUtils.Field[] fields = NYCTLCUtils.Field.values();
		for (Entry<Integer, BaseStat> fieldIdx : statsList.entrySet()) {
			statsJson.put(
					fields[fieldIdx.getKey()].getIndexedName(),
					fieldIdx.getValue().toJSONObject());
		}

		statsJson.put(
				"duration",
				durationStat.toJSONObject());

		return statsJson;
	}

	public static abstract class BaseStat<T> implements
			Mergeable
	{
		public abstract void updateStat(
				T value );

		public abstract void clear();

		public abstract JSONObject toJSONObject();

		public static abstract class BaseStatBuilder
		{
			public abstract BaseStat build();
		}
	}

	public static class MinMaxTotStat extends
			BaseStat<Number>
	{
		double minValue = Double.MAX_VALUE;
		double maxValue = -Double.MAX_VALUE;
		double totalValue = 0;
		long numValues = 0;

		@Override
		public void updateStat(
				Number value ) {
			Double doubleValue = value.doubleValue();
			if (doubleValue < minValue) minValue = doubleValue;
			if (doubleValue > maxValue) maxValue = doubleValue;
			totalValue += doubleValue;
			numValues++;
		}

		public void clear() {
			minValue = Double.MAX_VALUE;
			maxValue = -Double.MAX_VALUE;
			totalValue = 0;
			numValues = 0;
		}

		@Override
		public JSONObject toJSONObject() {
			final JSONObject jsonObj = new JSONObject();
			jsonObj.put(
					"min",
					minValue);
			jsonObj.put(
					"max",
					maxValue);
			jsonObj.put(
					"total",
					totalValue);
			jsonObj.put(
					"avg",
					totalValue / (double) numValues);
			return jsonObj;
		}

		@Override
		public void merge(
				Mergeable merge ) {
			if ((merge != null) && (merge instanceof MinMaxTotStat)) {
				MinMaxTotStat stat = (MinMaxTotStat) merge;
				if (stat.minValue < minValue) minValue = stat.minValue;
				if (stat.maxValue > maxValue) maxValue = stat.maxValue;
				totalValue += stat.totalValue;
				numValues += stat.numValues;
			}
		}

		@Override
		public byte[] toBinary() {
			final ByteBuffer buffer = ByteBuffer.allocate(32);
			buffer.putDouble(minValue);
			buffer.putDouble(maxValue);
			buffer.putDouble(totalValue);
			buffer.putLong(numValues);
			return buffer.array();
		}

		@Override
		public void fromBinary(
				byte[] bytes ) {
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			minValue = buffer.getDouble();
			maxValue = buffer.getDouble();
			totalValue = buffer.getDouble();
			numValues = buffer.getLong();
		}

		public static class MinMaxTotStatBuilder extends
				BaseStatBuilder
		{
			@Override
			public MinMaxTotStat build() {
				return new MinMaxTotStat();
			}
		}

		public double getMinValue() {
			return minValue;
		}

		public double getMaxValue() {
			return maxValue;
		}

		public double getTotalValue() {
			return totalValue;
		}

		public long getNumValues() {
			return numValues;
		}

		public double getAvgValue() {
			return totalValue / (double) numValues;
		}
	}

	public static class IntHistStat extends
			BaseStat<Integer>
	{
		int firstBin;
		int lastBin;
		long[] counts;

		public IntHistStat(
				int firstBin,
				int lastBin ) {
			this.firstBin = firstBin;
			this.lastBin = lastBin;
			this.counts = new long[lastBin - firstBin + 1];
			for (int bin = firstBin; bin <= lastBin; bin++)
				this.counts[bin - this.firstBin] = 0;
		}

		@Override
		public void updateStat(
				Integer bin ) {
			if (bin != null && 0 <= (bin - firstBin) && (bin - firstBin) < counts.length) {
				// check to make sure that value is in range
				counts[bin - firstBin]++;
			}
		}

		@Override
		public void clear() {
			for (int bin = firstBin; bin <= lastBin; bin++)
				this.counts[bin - firstBin] = 0;
		}

		@Override
		public JSONObject toJSONObject() {
			JSONObject jsonObj = new JSONObject();
			for (int bin = firstBin; bin <= lastBin; bin++)
				jsonObj.put(
						bin,
						counts[bin - firstBin]);
			return jsonObj;
		}

		@Override
		public void merge(
				Mergeable merge ) {
			if ((merge != null) && (merge instanceof IntHistStat)) {
				IntHistStat stat = (IntHistStat) merge;
				for (int bin = firstBin; bin <= lastBin; bin++)
					this.counts[bin - firstBin] += stat.counts[bin - firstBin];
			}
		}

		@Override
		public byte[] toBinary() {
			final ByteBuffer buffer = ByteBuffer.allocate(8 + counts.length * 8);
			buffer.putInt(firstBin);
			buffer.putInt(lastBin);
			for (int bin = firstBin; bin <= lastBin; bin++)
				buffer.putLong(this.counts[bin - firstBin]);
			return buffer.array();
		}

		@Override
		public void fromBinary(
				byte[] bytes ) {
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			firstBin = buffer.getInt();
			lastBin = buffer.getInt();
			this.counts = new long[lastBin - firstBin + 1];
			for (int bin = firstBin; bin <= lastBin; bin++)
				this.counts[bin - firstBin] = buffer.getLong();
		}

		public static class IntHistStatBuilder extends
				BaseStatBuilder
		{
			int firstBin;
			int lastBin;

			public IntHistStatBuilder(
					int firstBin,
					int lastBin ) {
				this.firstBin = firstBin;
				this.lastBin = lastBin;
			}

			@Override
			public IntHistStat build() {
				return new IntHistStat(
						firstBin,
						lastBin);
			}
		}
	}
}
