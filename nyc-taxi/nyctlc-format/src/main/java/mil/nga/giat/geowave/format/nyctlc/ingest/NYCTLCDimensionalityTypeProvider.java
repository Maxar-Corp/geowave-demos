package mil.nga.giat.geowave.format.nyctlc.ingest;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericValue;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeOptions;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils;
import mil.nga.giat.geowave.format.nyctlc.adapter.TimeOfDay;

public class NYCTLCDimensionalityTypeProvider implements
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

	public final static ByteArrayId TIME_OF_DAY_SEC_FIELD_ID = new ByteArrayId(
			ByteArrayUtils.combineArrays(
					mil.nga.giat.geowave.core.index.StringUtils.stringToBinary(NYCTLCUtils.Field.TIME_OF_DAY_SEC
							.getIndexedName()),
					new byte[] {
						0,
						0
					}));

	public NYCTLCDimensionalityTypeProvider() {}

	@Override
	public String getDimensionalityTypeName() {
		return "nyctlc_sst";
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
			new TimeOfDayField(
					new TimeOfDayDefinition(),
					TIME_OF_DAY_SEC_FIELD_ID)
		};

		final NumericDimensionDefinition[] dimensions = new NumericDimensionDefinition[] {
			new PickupLongitudeDefinition(),
			new PickupLatitudeDefinition(
					true),
			new DropoffLongitudeDefinition(),
			new DropoffLatitudeDefinition(
					true),
			new TimeOfDayDefinition()
		};

		final String combinedId = DEFAULT_NYCTLC_ID_STR + "_" + options.bias;

		return new CustomIdIndex(
				TieredSFCIndexFactory.createDefinedPrecisionTieredStrategy(
						dimensions,
						new int[][] {
							new int[] {
								0,
								options.bias.getSpatialPrecision()
							},
							new int[] {
								0,
								options.bias.getSpatialPrecision()
							},
							new int[] {
								0,
								options.bias.getSpatialPrecision()
							},
							new int[] {
								0,
								options.bias.getSpatialPrecision()
							},
							new int[] {
								0,
								options.bias.getTemporalPrecision()
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

	public static class TimeOfDayField implements
			NumericDimensionField<TimeOfDay>
	{
		private TimeOfDayDefinition base;
		private ByteArrayId fieldId;

		public TimeOfDayField() {
			super();
		}

		public TimeOfDayField(
				TimeOfDayDefinition base,
				ByteArrayId fieldId ) {
			super();
			this.base = base;
			this.fieldId = fieldId;
		}

		public ByteArrayId getFieldId() {
			return fieldId;
		}

		public double normalize(
				double value ) {
			return base.normalize(value);
		}

		public BinRange[] getNormalizedRanges(
				NumericData range ) {
			return base.getNormalizedRanges(range);
		}

		public NumericData getFullRange() {
			return base.getFullRange();
		}

		public double denormalize(
				double value ) {
			return base.denormalize(value);
		}

		public NumericRange getDenormalizedRange(
				BinRange range ) {
			return base.getDenormalizedRange(range);
		}

		public int getFixedBinIdSize() {
			return base.getFixedBinIdSize();
		}

		public double getRange() {
			return base.getRange();
		}

		public NumericRange getBounds() {
			return base.getBounds();
		}

		@Override
		public byte[] toBinary() {
			final byte[] dimensionBinary = PersistenceUtils.toBinary(base);
			final ByteBuffer buf = ByteBuffer.allocate(dimensionBinary.length + fieldId.getBytes().length + 4);
			buf.putInt(fieldId.getBytes().length);
			buf.put(fieldId.getBytes());
			buf.put(dimensionBinary);
			return buf.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			final int fieldIdLength = buf.getInt();
			final byte[] fieldIdBinary = new byte[fieldIdLength];
			buf.get(fieldIdBinary);
			fieldId = new ByteArrayId(
					fieldIdBinary);

			final byte[] dimensionBinary = new byte[bytes.length - fieldIdLength - 4];
			buf.get(dimensionBinary);
			base = PersistenceUtils.fromBinary(
					dimensionBinary,
					TimeOfDayDefinition.class);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((base == null) ? 0 : base.hashCode());
			result = (prime * result) + ((fieldId == null) ? 0 : fieldId.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final TimeOfDayField other = (TimeOfDayField) obj;
			if (base == null) {
				if (other.base != null) {
					return false;
				}
			}
			else if (!base.equals(other.base)) {
				return false;
			}
			if (fieldId == null) {
				if (other.fieldId != null) {
					return false;
				}
			}
			else if (!fieldId.equals(other.fieldId)) {
				return false;
			}
			return true;
		}

		@Override
		public NumericData getNumericData(
				TimeOfDay dataElement ) {
			return new NumericValue(
					dataElement.getTimeSeconds());
		}

		@Override
		public FieldWriter<?, TimeOfDay> getWriter() {
			return new FieldWriter<Object, TimeOfDay>() {

				@Override
				public byte[] getVisibility(
						Object rowValue,
						ByteArrayId fieldId,
						TimeOfDay fieldValue ) {
					return fieldValue.getVisibility();
				}

				@Override
				public byte[] writeField(
						TimeOfDay fieldValue ) {
					ByteBuffer buf = ByteBuffer.allocate(4);
					buf.putInt(fieldValue.getTimeSeconds());
					return buf.array();
				}
			};
		}

		@Override
		public FieldReader<TimeOfDay> getReader() {
			return new FieldReader<TimeOfDay>() {

				@Override
				public TimeOfDay readField(
						byte[] fieldData ) {
					ByteBuffer buf = ByteBuffer.wrap(fieldData);
					return new TimeOfDay(
							buf.getInt(),
							new byte[] {});

				}
			};
		}

		@Override
		public NumericDimensionDefinition getBaseDefinition() {
			return base;
		}
	}

	public static class TimeOfDayDefinition extends
			BasicDimensionDefinition
	{
		public TimeOfDayDefinition() {
			super(
					0,
					new Long(
							TimeUnit.DAYS.toSeconds(1)).doubleValue());
		}
	}
}
