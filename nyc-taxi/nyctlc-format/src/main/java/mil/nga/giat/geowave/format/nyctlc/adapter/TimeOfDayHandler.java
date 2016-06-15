package mil.nga.giat.geowave.format.nyctlc.adapter;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import mil.nga.giat.geowave.adapter.vector.FeatureAttributeHandler;
import mil.nga.giat.geowave.core.geotime.TimeUtils;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time.Timestamp;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;

public class TimeOfDayHandler implements
		IndexFieldHandler<SimpleFeature, TimeOfDay, Object>
{
	private final FeatureAttributeHandler nativeTimestampHandler;
	private final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler;
	private final ByteArrayId[] nativeFieldIds;

	public TimeOfDayHandler(
			final AttributeDescriptor timestampAttrDesc ) {
		this(
				timestampAttrDesc,
				null);
	}

	public TimeOfDayHandler(
			final AttributeDescriptor timestampAttrDesc,
			final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler ) {
		nativeTimestampHandler = new FeatureAttributeHandler(
				timestampAttrDesc);
		this.visibilityHandler = visibilityHandler;
		nativeFieldIds = new ByteArrayId[] {
			nativeTimestampHandler.getFieldId()
		};
	}

	@Override
	public ByteArrayId[] getNativeFieldIds() {
		return nativeFieldIds;
	}

	@Override
	public TimeOfDay toIndexValue(
			final SimpleFeature row ) {
		final Object object = nativeTimestampHandler.getFieldValue(row);
		byte[] visibility;
		if (visibilityHandler != null) {
			visibility = visibilityHandler.getVisibility(
					row,
					nativeTimestampHandler.getFieldId(),
					object);
		}
		else {
			visibility = new byte[] {};
		}
		return new TimeOfDay(
				((Number) object).intValue(),
				visibility);
	}

	@SuppressWarnings("unchecked")
	@Override
	public PersistentValue<Object>[] toNativeValues(
			final TimeOfDay indexValue ) {
		return new PersistentValue[] {
			new PersistentValue<Object>(
					nativeTimestampHandler.getFieldId(),
					new Integer(
							indexValue.getTimeSeconds()))
		};
	}
}