package mil.nga.giat.geowave.process.nyctlc;

import org.apache.accumulo.core.data.Key;
import org.apache.spark.serializer.KryoRegistrator;
import org.geotools.feature.simple.SimpleFeatureImpl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;

import mil.nga.giat.geowave.analytic.kryo.FeatureSerializer;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class GeoWaveRegistrator implements
		KryoRegistrator
{

	@Override
	public void registerClasses(
			final Kryo kryo ) {
		kryo.addDefaultSerializer(
				Persistable.class,
				new PersistableSerializer());
		kryo.register(Key.class);
		kryo.register(
				CustomIdIndex.class,
				new CustomIdSerializer());
		kryo.register(
				PrimaryIndex.class,
				new PersistableSerializer());
		kryo.register(
				SimpleFeatureImpl.class,
				new FeatureSerializer());

	}

	private static class CustomIdSerializer extends
			Serializer<CustomIdIndex>
	{

		@Override
		public void write(
				final Kryo kryo,
				final Output output,
				final CustomIdIndex geowaveObj ) {
			final byte[] bytes = PersistenceUtils.toBinary(geowaveObj);
			output.writeInt(bytes.length);
			output.writeBytes(bytes);
		}

		@Override
		public CustomIdIndex read(
				final Kryo kryo,
				final Input input,
				final Class<CustomIdIndex> type ) {
			final int length = input.readInt();
			final byte[] bytes = new byte[length];
			input.read(bytes);

			return PersistenceUtils.fromBinary(
					bytes,
					CustomIdIndex.class);
		}

		// }
	}

	// Default serializer for any GeoWave Persistable object
	private static class PersistableSerializer extends
			Serializer<Persistable>
	{

		@Override
		public void write(
				final Kryo kryo,
				final Output output,
				final Persistable geowaveObj ) {
			final byte[] bytes = PersistenceUtils.toBinary(geowaveObj);
			output.writeInt(bytes.length);
			output.writeBytes(bytes);
		}

		@Override
		public Persistable read(
				final Kryo kryo,
				final Input input,
				final Class<Persistable> type ) {
			final int length = input.readInt();
			final byte[] bytes = new byte[length];
			input.read(bytes);

			return PersistenceUtils.fromBinary(
					bytes,
					Persistable.class);
		}

		// }
	}
}
