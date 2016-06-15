package mil.nga.giat.geowave.format.nyctlc.query;

import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.format.nyctlc.statistics.NYCTLCParameters;
import mil.nga.giat.geowave.format.nyctlc.statistics.NYCTLCStatistics;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Created by geowave on 4/28/16.
 */
public class NYCTLCAggregation implements
		Aggregation<NYCTLCParameters, NYCTLCStatistics, SimpleFeature>
{
	private NYCTLCStatistics internalStats = new NYCTLCStatistics();

	private NYCTLCParameters internalParams = new NYCTLCParameters();

	public NYCTLCAggregation() {}

	@Override
	public NYCTLCParameters getParameters() {
		return internalParams;
	}

	@Override
	public void setParameters(
			NYCTLCParameters parameters ) {
		internalParams = parameters;
	}

	@Override
	public NYCTLCStatistics getResult() {
		return internalStats;
	}

	@Override
	public void clearResult() {
		internalStats = new NYCTLCStatistics();
	}

	@Override
	public void aggregate(
			SimpleFeature entry ) {
		internalStats.updateStats(
				entry,
				internalParams);
	}
}
