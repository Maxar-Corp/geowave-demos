package mil.nga.giat.geowave.format.nyctlc.ingest;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeOptions;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils;

public class MultiGeoMultiTimeDimensionalityTypeProvider implements
		DimensionalityTypeProviderSpi
{
	private final NYCTLCOptions options = new NYCTLCOptions();
	// BBox of NYC is more contrained, but we'll give it a little buffer
	// North Latitude: 40.915256 South Latitude: 40.496044 East Longitude:
	// -73.700272 West Longitude: -74.255735
	public static final double MIN_LAT = 40.3;
	public static final double MAX_LAT = 41.1;
	public static final double MIN_LON = -74.375;
	public static final double MAX_LON = -73.575;
	private static final String DEFAULT_NYCTLC_ID_STR = "NYCTLC_IDX";

	public final static ByteArrayId PICKUP_GEOMETRY_FIELD_ID = new ByteArrayId(
			ByteArrayUtils.combineArrays(
					mil.nga.giat.geowave.core.index.StringUtils.stringToBinary(NYCTLCUtils.Field.PICKUP_LOCATION
							.getIndexedName()),
					new byte[] {
						0,
						0
					}));

	public final static ByteArrayId DROPOFF_GEOMETRY_FIELD_ID = new ByteArrayId(
			ByteArrayUtils.combineArrays(
					mil.nga.giat.geowave.core.index.StringUtils.stringToBinary(NYCTLCUtils.Field.DROPOFF_LOCATION
							.getIndexedName()),
					new byte[] {
						0,
						0
					}));
	public final static ByteArrayId PICKUP_TIME_FIELD_ID = new ByteArrayId(
			ByteArrayUtils.combineArrays(
					mil.nga.giat.geowave.core.index.StringUtils.stringToBinary(NYCTLCUtils.Field.PICKUP_DATETIME
							.getIndexedName()),
					new byte[] {
						0,
						0
					}));

	public final static ByteArrayId DROPOFF_TIME_FIELD_ID = new ByteArrayId(
			ByteArrayUtils.combineArrays(
					mil.nga.giat.geowave.core.index.StringUtils.stringToBinary(NYCTLCUtils.Field.DROPOFF_DATETIME
							.getIndexedName()),
					new byte[] {
						0,
						0
					}));

	public MultiGeoMultiTimeDimensionalityTypeProvider() {}

	@Override
	public String getDimensionalityTypeName() {
		return "multigeo-multitime";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return "This dimensionality type is sued to index NYCTLC formatted data with indices for pickup location, dropoff location, and time of day.";
	}

	@Override
	public int getPriority() {
		// arbitrary - just lower than spatial so that the default
		// will be spatial over spatial-temporal
		return 5;
	}

	@Override
	public DimensionalityTypeOptions getOptions() {
		return options;
	}

	@Override
	public PrimaryIndex createPrimaryIndex() {
		return internalCreatePrimaryIndex(options);
	}

	private static PrimaryIndex internalCreatePrimaryIndex(
			final NYCTLCOptions options ) {

		final NumericDimensionField[] fields = new NumericDimensionField[] {
			new LongitudeField(
					PICKUP_GEOMETRY_FIELD_ID),
			new LatitudeField(
					true,
					PICKUP_GEOMETRY_FIELD_ID),
			new LongitudeField(
					DROPOFF_GEOMETRY_FIELD_ID),
			new LatitudeField(
					true,
					DROPOFF_GEOMETRY_FIELD_ID),
			new TimeField(
					Unit.YEAR,
					PICKUP_TIME_FIELD_ID),
			new TimeField(
					Unit.YEAR,
					DROPOFF_TIME_FIELD_ID)
		};

		final NumericDimensionDefinition[] dimensions = new NumericDimensionDefinition[] {
			new PickupLongitudeDefinition(),
			new PickupLatitudeDefinition(
					true),
			new DropoffLongitudeDefinition(),
			new DropoffLatitudeDefinition(
					true),
			new PickupTimeDefinition(),
			new DropoffTimeDefinition()
		};

		final String combinedId = DEFAULT_NYCTLC_ID_STR + "_" + options.bias;

		return new CustomIdIndex(
				TieredSFCIndexFactory.createDefinedPrecisionTieredStrategy(
						dimensions,
						new int[][] {
							new int[] {
								0,
								10
							},
							new int[] {
								0,
								10
							},
							new int[] {
								0,
								10
							},
							new int[] {
								0,
								10
							},
							new int[] {
								0,
								10
							},
							new int[] {
								0,
								10
							}
						},
						SFCFactory.SFCType.HILBERT),
				new BasicIndexModel(
						fields),
				new ByteArrayId(
						combinedId + "_POINTONLY"));
	}

	@Override
	public Class<? extends CommonIndexValue>[] getRequiredIndexTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}

	private static class NYCTLCOptions implements
			DimensionalityTypeOptions
	{
		@Parameter(names = {
			"--bias"
		}, required = false, description = "The bias of the index. There can be more precision given to time or space if necessary.", converter = BiasConverter.class)
		protected Bias bias = Bias.BALANCED;
	}

	public static enum Bias {
		TEMPORAL,
		BALANCED,
		SPATIAL;
		// converter that will be used later
		public static Bias fromString(
				final String code ) {

			for (final Bias output : Bias.values()) {
				if (output.toString().equalsIgnoreCase(
						code)) {
					return output;
				}
			}

			return null;
		}

		protected int getSpatialPrecision() {
			switch (this) {
				case SPATIAL:
					return 14;
				case TEMPORAL:
					return 7;
				case BALANCED:
				default:
					return 12;
			}
		}

		protected int getTemporalPrecision() {
			switch (this) {
				case SPATIAL:
					return 6;
				case TEMPORAL:
					return 34;
				case BALANCED:
				default:
					return 12;
			}
		}
	}

	public static class BiasConverter implements
			IStringConverter<Bias>
	{
		@Override
		public Bias convert(
				final String value ) {
			final Bias convertedValue = Bias.fromString(value);

			if (convertedValue == null) {
				throw new ParameterException(
						"Value " + value + "can not be converted to an index bias. " + "Available values are: "
								+ StringUtils.join(
										Bias.values(),
										", ").toLowerCase());
			}
			return convertedValue;
		}

	}

	public static class NYCTLCIndexBuilder
	{
		private final NYCTLCOptions options;

		public NYCTLCIndexBuilder() {
			options = new NYCTLCOptions();
		}

		private NYCTLCIndexBuilder(
				final NYCTLCOptions options ) {
			this.options = options;
		}

		public NYCTLCIndexBuilder setBias(
				final Bias bias ) {
			options.bias = bias;
			return new NYCTLCIndexBuilder(
					options);
		}

		public PrimaryIndex createIndex() {
			return internalCreatePrimaryIndex(options);
		}
	}

	public static class PickupLongitudeDefinition extends
			LongitudeDefinition
	{
		public PickupLongitudeDefinition() {
			super();
			min = MIN_LON;
			max = MAX_LON;
		}

		@Override
		public BinRange[] getNormalizedRanges(
				final NumericData range ) {
			return new BinRange[] {
				new BinRange(
						// by default clamp to the min and max
						clamp(range.getMin()),
						clamp(range.getMax()))
			};
		}
	}

	public static class PickupTimeDefinition extends
			TimeDefinition
	{
		public PickupTimeDefinition() {
			super(
					Unit.YEAR);
		}
	}

	public static class DropoffTimeDefinition extends
			TimeDefinition
	{
		public DropoffTimeDefinition() {
			super(
					Unit.YEAR);
		}
	}

	public static class PickupLatitudeDefinition extends
			LatitudeDefinition
	{
		public PickupLatitudeDefinition() {
			super();
			min = MIN_LAT;
			max = MAX_LAT;
		}

		public PickupLatitudeDefinition(
				final boolean useHalfRange ) {
			super(
					useHalfRange);
			min = MIN_LAT;
			max = MAX_LAT;
		}

		@Override
		protected double clamp(
				final double x ) {
			return clamp(
					x,
					MIN_LAT,
					MAX_LAT);
		}
	}

	public static class DropoffLongitudeDefinition extends
			LongitudeDefinition
	{
		public DropoffLongitudeDefinition() {
			super();
			min = MIN_LON;
			max = MAX_LON;
		}

		@Override
		public BinRange[] getNormalizedRanges(
				final NumericData range ) {
			return new BinRange[] {
				new BinRange(
						// by default clamp to the min and max
						clamp(range.getMin()),
						clamp(range.getMax()))
			};
		}
	}

	public static class DropoffLatitudeDefinition extends
			LatitudeDefinition
	{
		public DropoffLatitudeDefinition() {
			min = MIN_LAT;
			max = MAX_LAT;
		}

		public DropoffLatitudeDefinition(
				final boolean useHalfRange ) {
			super(
					useHalfRange);
			min = MIN_LAT;
			max = MAX_LAT;
		}

		@Override
		protected double clamp(
				final double x ) {
			return clamp(
					x,
					MIN_LAT,
					MAX_LAT);
		}
	}
}
