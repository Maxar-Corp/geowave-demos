package mil.nga.giat.geowave.format.nyctlc.cli;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;

@GeowaveOperation(name = "nyctlc", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Experimentation with NYCTLC data")
public class NYCTLCSection extends
		DefaultOperation
{

}
