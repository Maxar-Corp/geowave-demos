package mil.nga.giat.geowave.format.nyctlc;

import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestFormat;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;
import mil.nga.giat.geowave.format.nyctlc.avro.NYCTLCEntry;

public class NYCTLCIngestFormat extends
		AbstractSimpleFeatureIngestFormat<NYCTLCEntry>
{

	@Override
	protected AbstractSimpleFeatureIngestPlugin<NYCTLCEntry> newPluginInstance(
			IngestFormatOptionProvider options ) {
		return new NYCTLCIngestPlugin();
	}

	@Override
	public String getIngestFormatName() {
		return "nyctlc";
	}

	@Override
	public String getIngestFormatDescription() {
		return "files from New York City Taxi & Limousine Commission data set";
	}
}
