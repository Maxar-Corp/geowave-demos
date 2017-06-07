package mil.nga.giat.geowave.format.nyctlc.cli;

import java.util.Arrays;

public class Statistics
{
	double[] data;
	int size;
	private String storeName;
	private String queryName;
	private final long entryCount;

	public Statistics(
			final double[] data,
			final long entryCount,
			String storeName,
			String queryName ) {
		this.data = data;
		this.entryCount = entryCount;
		size = data.length;
		this.storeName = storeName;
	}

	double getMean() {
		double sum = 0.0;
		for (final double a : data) {
			sum += a;
		}
		return sum / size;
	}

	double getVariance() {
		final double mean = getMean();
		double temp = 0;
		for (final double a : data) {
			temp += (a - mean) * (a - mean);
		}
		return temp / size;
	}

	double getStdDev() {
		return Math.sqrt(getVariance());
	}

	public double median() {
		Arrays.sort(data);

		if ((data.length % 2) == 0) {
			return (data[(data.length / 2) - 1] + data[data.length / 2]) / 2.0;
		}
		return data[data.length / 2];
	}

	@Override
	public String toString() {
		return "Statistics [data=" + Arrays.toString(data) + ", size=" + size + ", entryCount=" + entryCount + "]\n"
				+ toCSVRow();
	}

	public static String getCSVHeader() {
		return "Range Count, Result Count, Mean (ms), Median (ms), Std Dev (ms), store, query";
	}

	public String toCSVRow() {
		return String.format(
				"%s,%s,%s,%s,%s,%s",
				entryCount,
				getMean(),
				median(),
				getStdDev(),
				storeName,
				queryName);
	}

	public static void printStats(
			final Statistics stats ) {
		// TODO write it to a file instead
		if (stats != null) {
			stats.printStats();
		}

	}

	public void printStats() {
		// TODO write it to a file instead
		System.err.println(toCSVRow());
	}
}
