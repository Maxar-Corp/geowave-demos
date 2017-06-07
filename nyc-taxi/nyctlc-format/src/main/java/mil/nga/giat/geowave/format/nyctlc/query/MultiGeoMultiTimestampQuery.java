package mil.nga.giat.geowave.format.nyctlc.query;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
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
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintData;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintSet;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.format.nyctlc.ingest.MultiGeoMultiTimeDimensionalityTypeProvider;
import mil.nga.giat.geowave.format.nyctlc.ingest.MultiGeoMultiTimeDimensionalityTypeProvider.DropoffTimeDefinition;
import mil.nga.giat.geowave.format.nyctlc.ingest.MultiGeoMultiTimeDimensionalityTypeProvider.PickupTimeDefinition;
import mil.nga.giat.geowave.format.nyctlc.ingest.NYCTLCDimensionalityTypeProvider;
import mil.nga.giat.geowave.format.nyctlc.ingest.NYCTLCDimensionalityTypeProvider.TimeOfDayDefinition;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by geowave on 4/28/16.
 */
public class MultiGeoMultiTimestampQuery extends
		BasicQuery
{

	private final static Logger LOGGER = Logger.getLogger(MultiGeoMultiTimestampQuery.class);
	private Geometry pickupGeometry;
	private Geometry dropoffGeometry;
	SpatialQueryFilter.CompareOperation compareOp = SpatialQueryFilter.CompareOperation.OVERLAPS;

	protected MultiGeoMultiTimestampQuery() {}

	public MultiGeoMultiTimestampQuery(
			final Date pickupStartTime,
			final Date pickupEndTime,
			final Date dropoffStartTime,
			final Date dropoffEndTime,
			final Geometry pickupGeometry,
			final Geometry dropoffGeometry ) {
		super(
				createNYCTLCConstraints(
						pickupStartTime,
						pickupEndTime,
						dropoffStartTime,
						dropoffEndTime,
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

	private static Constraints createNYCTLCConstraints(
			final Date pickupStartTime,
			final Date pickupEndTime,
			final Date dropoffStartTime,
			final Date dropoffEndTime,
			final Geometry pickupGeometry,
			final Geometry dropoffGeometry ) {

		Constraints pickupConstraints = new Constraints(
				new ConstraintSet(
						PickupTimeDefinition.class,
						new ConstraintData(
								new NumericRange(
										pickupStartTime.getTime(),
										pickupEndTime.getTime()),
								false)));

		Constraints dropoffConstraints = new Constraints(
				new ConstraintSet(
						DropoffTimeDefinition.class,
						new ConstraintData(
								new NumericRange(
										dropoffStartTime.getTime(),
										dropoffEndTime.getTime()),
								false)));
		final Constraints geoConstraints = basicConstraintsFromGeometry(
				pickupGeometry,
				dropoffGeometry);
		return geoConstraints.merge(
				pickupConstraints).merge(
				dropoffConstraints);
	}

	private static CommonIndexModel im = new MultiGeoMultiTimeDimensionalityTypeProvider()
			.createPrimaryIndex()
			.getIndexModel();

	@Override
	public List<QueryFilter> createFilters(
			CommonIndexModel indexModel ) {

		return super.createFilters(im);
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
