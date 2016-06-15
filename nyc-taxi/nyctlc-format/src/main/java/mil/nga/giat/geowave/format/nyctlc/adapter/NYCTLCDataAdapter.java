package mil.nga.giat.geowave.format.nyctlc.adapter;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.FeatureGeometryHandler;
import mil.nga.giat.geowave.adapter.vector.FeatureTimestampHandler;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.VisibilityConfiguration;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.VisibilityManagement;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils;
import mil.nga.giat.geowave.format.nyctlc.ingest.NYCTLCDimensionalityTypeProvider;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import java.util.List;

/**
 * Created by geowave on 4/27/16.
 */
public class NYCTLCDataAdapter extends
		FeatureDataAdapter
{
	protected NYCTLCDataAdapter() {}

	public NYCTLCDataAdapter(
			SimpleFeatureType type ) {
		super(
				type);
	}

	public NYCTLCDataAdapter(
			SimpleFeatureType type,
			VisibilityManagement<SimpleFeature> visibilityManagement ) {
		super(
				type,
				visibilityManagement);
	}

	public NYCTLCDataAdapter(
			SimpleFeatureType type,
			List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers ) {
		super(
				type,
				customIndexHandlers);
	}

	public NYCTLCDataAdapter(
			SimpleFeatureType type,
			FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler ) {
		super(
				type,
				fieldVisiblityHandler);
	}

	public NYCTLCDataAdapter(
			SimpleFeatureType type,
			List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers,
			FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler,
			VisibilityManagement<SimpleFeature> defaultVisibilityManagement ) {
		super(
				type,
				customIndexHandlers,
				fieldVisiblityHandler,
				defaultVisibilityManagement);
	}

	@Override
	protected IndexFieldHandler<SimpleFeature, Time, Object> getTimeRangeHandler(
			SimpleFeatureType featureType ) {
		return null;
	}

	@Override
	public boolean hasTemporalConstraints() {
		return false;
	}

	@Override
	public synchronized TimeDescriptors getTimeDescriptors() {
		return new TimeDescriptors();
	}

	@Override
	public synchronized void resetTimeDescriptors() {}

	@Override
	protected void init(
			List<? extends IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> indexFieldHandlers,
			Object defaultIndexHandlerData ) {
		super.init(
				indexFieldHandlers,
				defaultIndexHandlerData);

		final SimpleFeatureType type = (SimpleFeatureType) defaultIndexHandlerData;

		final VisibilityConfiguration config = new VisibilityConfiguration(
				type);

		// Set Pickup Geometry Descriptor
		final AttributeDescriptor pickupDescriptor = type.getDescriptor(NYCTLCUtils.Field.PICKUP_LOCATION
				.getIndexedName());
		dimensionMatchingFieldHandlers.put(
				NYCTLCDimensionalityTypeProvider.PICKUP_GEOMETRY_FIELD_ID,
				new FeatureGeometryHandler(
						pickupDescriptor,
						config.getManager().createVisibilityHandler(
								pickupDescriptor.getLocalName(),
								fieldVisiblityHandler,
								config.getAttributeName())));

		// Set Dropoff Geometry Descriptor
		final AttributeDescriptor dropoffLonDescriptor = type.getDescriptor(NYCTLCUtils.Field.DROPOFF_LOCATION
				.getIndexedName());
		dimensionMatchingFieldHandlers.put(
				NYCTLCDimensionalityTypeProvider.DROPOFF_GEOMETRY_FIELD_ID,
				new FeatureGeometryHandler(
						dropoffLonDescriptor,
						config.getManager().createVisibilityHandler(
								dropoffLonDescriptor.getLocalName(),
								fieldVisiblityHandler,
								config.getAttributeName())));

		final AttributeDescriptor timeOfDayDescriptor = type.getDescriptor(NYCTLCUtils.Field.TIME_OF_DAY_SEC
				.getIndexedName());
		dimensionMatchingFieldHandlers.put(
				NYCTLCDimensionalityTypeProvider.TIME_OF_DAY_SEC_FIELD_ID,
				new TimeOfDayHandler(
						timeOfDayDescriptor,
						config.getManager().createVisibilityHandler(
								timeOfDayDescriptor.getLocalName(),
								fieldVisiblityHandler,
								config.getAttributeName())));
	}
}
