package mil.nga.giat.geowave.format.nyctlc;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;

public class NYCTLCOptionProvider implements
		IngestFormatOptionProvider
{
	@Parameter(names = "--dropoff", required = false)
	public boolean dropoff = false;
	@Parameter(names = "--timerange", required = false)
	private boolean timerange = false;

	public NYCTLCOptionProvider() {}

	public boolean isDropoff() {
		return dropoff;
	}

	public boolean isTimerange() {
		return timerange;
	}
}
