package mil.nga.giat.geowave.format.nyctlc.cli;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class NYCTLCOperationProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		NYCTLCSection.class,
		NYCTLCQueryCommand.class,
		AutomateYankeesExperimentCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}