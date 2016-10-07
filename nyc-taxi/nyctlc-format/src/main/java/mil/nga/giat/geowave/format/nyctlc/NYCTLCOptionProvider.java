package mil.nga.giat.geowave.format.nyctlc;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;

public class NYCTLCOptionProvider implements
		IngestFormatOptionProvider
{
	@Parameter(names = "--dropoff", required = false)
	private boolean dropoff = false;

	public NYCTLCOptionProvider() {}

	public boolean isDropoff() {
		return dropoff;
	}
}
