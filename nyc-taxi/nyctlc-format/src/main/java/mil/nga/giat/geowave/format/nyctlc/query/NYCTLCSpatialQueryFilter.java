package mil.nga.giat.geowave.format.nyctlc.query;

import java.util.Date;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

public class NYCTLCSpatialQueryFilter extends
		SpatialQuery
{

	private Date pickupStartTime;
	private Date pickupEndTime;
	private Date dropoffStartTime;
	private Date dropoffEndTime;

	public NYCTLCSpatialQueryFilter(
			Geometry queryGeometry,
			Geometry otherGeometry,
			Date pickupStartTime,
			Date pickupEndTime,
			Date dropoffStartTime,
			Date dropoffEndTime ) {
		super(
				queryGeometry);
		this.pickupStartTime = pickupStartTime;
		this.pickupEndTime = pickupEndTime;
		this.dropoffStartTime = dropoffStartTime;
		this.dropoffEndTime = dropoffEndTime;
	}

	@Override
	protected DistributableQueryFilter createQueryFilter(
			MultiDimensionalNumericData constraints,
			NumericDimensionField<?>[] orderedConstrainedDimensionFields,
			NumericDimensionField<?>[] unconstrainedDimensionDefinitions ) {
		// TODO Auto-generated method stub
		return super.createQueryFilter(
				constraints,
				orderedConstrainedDimensionFields,
				unconstrainedDimensionDefinitions);
	}

	@Override
	public byte[] toBinary() {
		// TODO Auto-generated method stub
		return super.toBinary();
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		super.fromBinary(bytes);
	}

}
