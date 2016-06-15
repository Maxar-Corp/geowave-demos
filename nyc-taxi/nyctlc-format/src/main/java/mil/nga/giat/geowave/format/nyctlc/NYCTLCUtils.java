package mil.nga.giat.geowave.format.nyctlc;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.format.nyctlc.statistics.NYCTLCStatistics;

/**
 * Created by geowave on 4/25/16.
 */
public class NYCTLCUtils
{

	public static final String NYCTLC_POINT_FEATURE = "nyctlcpoint";

	public static final Set<Field> REQUIRED_FIELDS = Collections.unmodifiableSet(new HashSet<Field>(
			Arrays.asList(new Field[] {
				Field.DROPOFF_LONGITUDE,
				Field.DROPOFF_LATITUDE,
				Field.PICKUP_LONGITUDE,
				Field.PICKUP_LATITUDE,
				Field.PICKUP_DATETIME
			})));

	public enum Field {
		VENDOR_ID(
				Integer.class,
				new String[] {
					"vendor"
				},
				false,
				new NYCTLCStatistics.IntHistStat.IntHistStatBuilder(
						0,
						2)),
		PICKUP_DATETIME(
				Date.class,
				new String[] {
					"pickupdatetime"
				},
				false,
				null),
		DROPOFF_DATETIME(
				Date.class,
				new String[] {
					"dropoffdatetime"
				},
				false,
				null),
		PASSENGER_COUNT(
				Integer.class,
				new String[] {
					"passengercount"
				},
				false,
				new NYCTLCStatistics.MinMaxTotStat.MinMaxTotStatBuilder()),
		TRIP_DISTANCE(
				Double.class,
				new String[] {
					"tripdistance"
				},
				false,
				new NYCTLCStatistics.MinMaxTotStat.MinMaxTotStatBuilder()),
		PICKUP_LONGITUDE(
				Double.class,
				new String[] {
					"pickuplongitude",
					"startlon"
				},
				false,
				null),
		PICKUP_LATITUDE(
				Double.class,
				new String[] {
					"pickuplatitude",
					"startlat"
				},
				false,
				null),
		PICKUP_LOCATION(
				Geometry.class,
				new String[] {},
				false,
				null),
		STORE_AND_FWD_FLAG(
				Boolean.class,
				new String[] {
					"storeandfwdflag",
					"storeandforward"
				},
				false,
				null),
		RATE_CODE_ID(
				Integer.class,
				new String[] {
					"ratecode"
				},
				false,
				new NYCTLCStatistics.IntHistStat.IntHistStatBuilder(
						1,
						6)),
		DROPOFF_LONGITUDE(
				Double.class,
				new String[] {
					"dropofflongitude",
					"endlon"
				},
				false,
				null),
		DROPOFF_LATITUDE(
				Double.class,
				new String[] {
					"dropofflatitude",
					"endlat"
				},
				false,
				null),
		DROPOFF_LOCATION(
				Geometry.class,
				new String[] {},
				false,
				null),
		PAYMENT_TYPE(
				Integer.class,
				new String[] {
					"paymenttype"
				},
				false,
				new NYCTLCStatistics.IntHistStat.IntHistStatBuilder(
						0,
						6)),
		FARE_AMOUNT(
				Double.class,
				new String[] {
					"fare"
				},
				false,
				new NYCTLCStatistics.MinMaxTotStat.MinMaxTotStatBuilder()),
		EXTRA(
				Double.class,
				new String[] {
					"extra"
				},
				false,
				null),
		MTA_TAX(
				Double.class,
				new String[] {
					"mtatax"
				},
				false,
				null),
		IMPROVEMENT_SURCHARGE(
				Double.class,
				new String[] {
					"surcharge"
				},
				false,
				null),
		TIP_AMOUNT(
				Double.class,
				new String[] {
					"tip"
				},
				false,
				new NYCTLCStatistics.MinMaxTotStat.MinMaxTotStatBuilder()),
		TOLLS_AMOUNT(
				Double.class,
				new String[] {
					"tolls"
				},
				false,
				new NYCTLCStatistics.MinMaxTotStat.MinMaxTotStatBuilder()),
		TOTAL_AMOUNT(
				Double.class,
				new String[] {
					"total"
				},
				false,
				new NYCTLCStatistics.MinMaxTotStat.MinMaxTotStatBuilder()),
		TRIP_TYPE(
				Integer.class,
				new String[] {
					"triptype"
				},
				true,
				null), // green taxi only
		EHAIL_FEE(
				Integer.class,
				new String[] {
					"ehailfee"
				},
				true,
				null), // green taxi only
		TIME_OF_DAY_SEC(
				Long.class,
				new String[] {},
				false,
				null), // derived field
		CAB_TYPE(
				Integer.class,
				new String[] {},
				false,
				new NYCTLCStatistics.IntHistStat.IntHistStatBuilder(
						0,
						2)); // derived field

		private final Class clazz;
		private final String[] variants;
		private final Boolean nillable;
		private final NYCTLCStatistics.BaseStat.BaseStatBuilder statBuilder;

		Field(
				final Class clazz,
				final String[] variants,
				final Boolean nillable,
				final NYCTLCStatistics.BaseStat.BaseStatBuilder statBuilder ) {
			this.clazz = clazz;
			this.variants = variants;
			this.nillable = nillable;
			this.statBuilder = statBuilder;
		}

		public String getIndexedName() {
			return name().toLowerCase();
		}

		public Class getBinding() {
			return clazz;
		}

		public NYCTLCStatistics.BaseStat.BaseStatBuilder getStatBuilder() {
			return statBuilder;
		}

		public String[] variants() {
			return variants;
		}

		public Boolean isNillable() {
			return nillable;
		}

		public boolean matches(
				final String fieldName ) {
			for (final String variant : variants()) {
				if (fieldName.replace(
						"_",
						"").replace(
						" ",
						"").toLowerCase().contains(
						variant)) {
					return true;
				}
			}
			return false;
		}

		public static Field getField(
				final String fieldName ) {
			final String normalizedFieldName = normalizeFieldName(fieldName);
			for (final Field field : Field.values()) {
				if (field.matches(normalizedFieldName)) {
					return field;
				}
			}
			return null;
		}

		private static final String normalizeFieldName(
				final String fieldName ) {
			return fieldName.replace(
					"_",
					"").replace(
					" ",
					"").toLowerCase();
		}
	}

	public static SimpleFeatureType createPointDataType() {
		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(NYCTLC_POINT_FEATURE);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		for (final Field field : Field.values()) {
			simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
					field.getBinding()).nillable(
					field.isNillable()).buildDescriptor(
					field.getIndexedName()));
		}

		return simpleFeatureTypeBuilder.buildFeatureType();
	}

	public static boolean validate(
			final File file ) {
		return true;
	}

	private static final String normalizeHeader(
			final String header ) {
		return header.replace(
				" ",
				"").replace(
				"_",
				"").toLowerCase();
	}
}
