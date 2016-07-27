package mil.nga.giat.geowave.format.nyctlc.query;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter;
import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.format.nyctlc.ingest.NYCTLCDimensionalityTypeProvider;
import mil.nga.giat.geowave.format.nyctlc.ingest.NYCTLCDimensionalityTypeProvider.TimeOfDayDefinition;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by geowave on 4/28/16.
 */
public class NYCTLCQuery extends
		BasicQuery
{

	private final static Logger LOGGER = Logger.getLogger(NYCTLCQuery.class);
	private Geometry pickupGeometry;
	private Geometry dropoffGeometry;
	SpatialQueryFilter.CompareOperation compareOp = SpatialQueryFilter.CompareOperation.OVERLAPS;

	protected NYCTLCQuery() {}

	public NYCTLCQuery(
			final int startTime,
			final int endTime,
			final Geometry pickupGeometry,
			final Geometry dropoffGeometry ) {
		super(
				createNYCTLCConstraints(
						new ConstraintData(
								new NumericRange(
										startTime,
										endTime),
								false),
						pickupGeometry,
						dropoffGeometry));
		this.pickupGeometry = pickupGeometry;
		this.dropoffGeometry = dropoffGeometry;
	}

	public Geometry getPickupGeometry() {
		return pickupGeometry;
	}

	public Geometry getDropoffGeometry() {
		return dropoffGeometry;
	}

	@Override
	protected DistributableQueryFilter createQueryFilter(
			final MultiDimensionalNumericData constraints,
			final NumericDimensionField<?>[] dimensionFields,
			final NumericDimensionField<?>[] UCdimensionFields ) {
		final List<NumericData> pickupConstraints = new ArrayList<NumericData>();
		final List<NumericData> dropoffConstraints = new ArrayList<NumericData>();
		final List<NumericDimensionField> pickupDimFields = new ArrayList<NumericDimensionField>();
		final List<NumericDimensionField> dropoffDimFields = new ArrayList<NumericDimensionField>();
		final List<NumericDimensionField> UCpickupDimFields = new ArrayList<NumericDimensionField>();
		final List<NumericDimensionField> UCdropoffDimFields = new ArrayList<NumericDimensionField>();

		for (int dim = 0; dim < constraints.getDimensionCount() && dim < dimensionFields.length; dim++) {
			if (!dimensionFields[dim].getFieldId().equals(
					NYCTLCDimensionalityTypeProvider.DROPOFF_GEOMETRY_FIELD_ID)) {
				pickupConstraints.add(constraints.getDataPerDimension()[dim]);
				pickupDimFields.add(dimensionFields[dim]);
			}
			else if (!dimensionFields[dim].getFieldId().equals(
					NYCTLCDimensionalityTypeProvider.PICKUP_GEOMETRY_FIELD_ID)) {
				dropoffConstraints.add(constraints.getDataPerDimension()[dim]);
				dropoffDimFields.add(dimensionFields[dim]);
			}
		}

		for (int dim = 0; dim < UCdimensionFields.length; dim++) {
			if (!UCdimensionFields[dim].getFieldId().equals(
					NYCTLCDimensionalityTypeProvider.DROPOFF_GEOMETRY_FIELD_ID)) {
				UCpickupDimFields.add(UCdimensionFields[dim]);
			}
			else if (!UCdimensionFields[dim].getFieldId().equals(
					NYCTLCDimensionalityTypeProvider.PICKUP_GEOMETRY_FIELD_ID)) {
				UCdropoffDimFields.add(UCdimensionFields[dim]);
			}
		}

		return new DistributableFilterList(
				true,
				Arrays.asList(new DistributableQueryFilter[] {
					new SpatialQueryFilter(
							new BasicNumericDataset(
									pickupConstraints.toArray(new NumericData[pickupConstraints.size()])),
							pickupDimFields.toArray(new NumericDimensionField[pickupDimFields.size()]),
							UCpickupDimFields.toArray(new NumericDimensionField[UCpickupDimFields.size()]),
							pickupGeometry,
							compareOp),
					new SpatialQueryFilter(
							new BasicNumericDataset(
									dropoffConstraints.toArray(new NumericData[dropoffConstraints.size()])),
							dropoffDimFields.toArray(new NumericDimensionField[dropoffDimFields.size()]),
							UCdropoffDimFields.toArray(new NumericDimensionField[UCdropoffDimFields.size()]),
							dropoffGeometry,
							compareOp)
				}));
	}

	private static Constraints createNYCTLCConstraints(
			final ConstraintData constraintData,
			final Geometry pickupGeometry,
			final Geometry dropoffGeometry ) {

		final ConstraintSet cs1 = new ConstraintSet();
		cs1.addConstraint(
				TimeOfDayDefinition.class,
				constraintData);

		final Constraints geoConstraints = basicConstraintsFromGeometry(
				pickupGeometry,
				dropoffGeometry);
		return geoConstraints.merge(new Constraints(
				cs1));
	}

	public static BasicQuery.Constraints basicConstraintsFromGeometry(
			final Geometry pickupGeometry,
			final Geometry dropoffGeometry ) {

		List<BasicQuery.ConstraintSet> pickupSet = new LinkedList<ConstraintSet>();
		constructListOfConstraintSetsFromGeometry(
				pickupGeometry,
				pickupSet,
				NYCTLCDimensionalityTypeProvider.PickupLongitudeDefinition.class,
				NYCTLCDimensionalityTypeProvider.PickupLatitudeDefinition.class);
		List<BasicQuery.ConstraintSet> dropoffSet = new LinkedList<BasicQuery.ConstraintSet>();
		constructListOfConstraintSetsFromGeometry(
				dropoffGeometry,
				dropoffSet,
				NYCTLCDimensionalityTypeProvider.DropoffLongitudeDefinition.class,
				NYCTLCDimensionalityTypeProvider.DropoffLatitudeDefinition.class);
		return new BasicQuery.Constraints(
				pickupSet).merge(new BasicQuery.Constraints(
				dropoffSet));
	}

	private static void constructListOfConstraintSetsFromGeometry(
			final Geometry geometry,
			final List<BasicQuery.ConstraintSet> destinationListOfSets,
			final Class lonClass,
			final Class latClass ) {

		// Get the envelope of the geometry being held
		int n = geometry.getNumGeometries();
		if (n > 1) {
			for (int gi = 0; gi < n; gi++) {
				constructListOfConstraintSetsFromGeometry(
						geometry.getGeometryN(gi),
						destinationListOfSets,
						lonClass,
						latClass);
			}
		}
		else {
			final Envelope env = geometry.getEnvelopeInternal();
			destinationListOfSets.add(constraintSetFromEnvelope(
					env,
					lonClass,
					latClass));
		}
	}

	public static BasicQuery.ConstraintSet constraintSetFromEnvelope(
			final Envelope env,
			final Class lonClass,
			final Class latClass ) {

		// Create a NumericRange object using the x axis
		final NumericRange rangeLongitude = new NumericRange(
				env.getMinX(),
				env.getMaxX());

		// Create a NumericRange object using the y axis
		final NumericRange rangeLatitude = new NumericRange(
				env.getMinY(),
				env.getMaxY());

		final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerDimension = new HashMap<Class<? extends NumericDimensionDefinition>, ConstraintData>();
		// Create and return a new IndexRange array with an x and y axis
		// range
		constraintsPerDimension.put(
				lonClass,
				new BasicQuery.ConstraintData(
						rangeLongitude,
						false));
		constraintsPerDimension.put(
				latClass,
				new BasicQuery.ConstraintData(
						rangeLatitude,
						false));
		return new BasicQuery.ConstraintSet(
				constraintsPerDimension);
	}

	@Override
	public byte[] toBinary() {
		final byte[] superBinary = super.toBinary();
		final byte[] pickupGeometryBinary = new WKBWriter().write(pickupGeometry);
		final byte[] dropoffGeometryBinary = new WKBWriter().write(dropoffGeometry);
		final ByteBuffer buf = ByteBuffer.allocate(superBinary.length + pickupGeometryBinary.length
				+ dropoffGeometryBinary.length + 16);
		buf.putInt(compareOp.ordinal());
		buf.putInt(superBinary.length);
		buf.put(superBinary);
		buf.putInt(pickupGeometryBinary.length);
		buf.put(pickupGeometryBinary);
		buf.putInt(dropoffGeometryBinary.length);
		buf.put(dropoffGeometryBinary);

		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		compareOp = SpatialQueryFilter.CompareOperation.values()[buf.getInt()];
		final int superLength = buf.getInt();
		final byte[] superBinary = new byte[superLength];
		buf.get(superBinary);
		super.fromBinary(superBinary);
		final int pickupGeomLength = buf.getInt();
		final byte[] pickupGeometryBinary = new byte[pickupGeomLength];
		buf.get(pickupGeometryBinary);
		try {
			pickupGeometry = new WKBReader().read(pickupGeometryBinary);
		}
		catch (final ParseException e) {
			LOGGER.warn(
					"Unable to read pickup query geometry as well-known binary",
					e);
		}
		final int dropoffGeomLength = buf.getInt();
		final byte[] dropoffGeometryBinary = new byte[dropoffGeomLength];
		buf.get(dropoffGeometryBinary);
		try {
			dropoffGeometry = new WKBReader().read(dropoffGeometryBinary);
		}
		catch (final ParseException e) {
			LOGGER.warn(
					"Unable to read dropoff query geometry as well-known binary",
					e);
		}
	}
}
