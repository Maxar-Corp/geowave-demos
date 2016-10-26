package mil.nga.giat.geowave.process.nyctlc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

public class TDigestSerializable implements
		Serializable,
		Writable,
		KryoSerializable
{
	private static final long serialVersionUID = 1L;
	protected AVLTreeDigest tdigest;

	private static final int MAX_CACHED_CDFS = 500;
	private Map<Double, Double> cdfCache;

	public TDigestSerializable() {
		tdigest = (AVLTreeDigest) TDigest.createAvlTreeDigest(100);
	}

	private void writeObject(
			final ObjectOutputStream oos )
			throws IOException {
		write(oos);
	}

	private void readObject(
			final ObjectInputStream ois )
			throws ClassNotFoundException,
			IOException {
		readFields(ois);
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		final int byteSize = in.readInt();
		final byte[] bytes = new byte[byteSize];
		in.readFully(bytes);
		tdigest = AVLTreeDigest.fromBytes(ByteBuffer.wrap(bytes));
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		final int byteSize = tdigest.byteSize();
		final ByteBuffer buf = ByteBuffer.allocate(byteSize);
		tdigest.asBytes(buf);
		out.writeInt(byteSize);
		out.write(buf.array());

	}

	@Override
	public void write(
			final Kryo kryo,
			final Output output ) {
		final int byteSize = tdigest.byteSize();
		final ByteBuffer buf = ByteBuffer.allocate(byteSize);
		tdigest.asBytes(buf);
		output.writeInt(byteSize);
		output.write(buf.array());
	}

	@Override
	public void read(
			final Kryo kryo,
			final Input input ) {
		final int byteSize = input.readInt();
		final byte[] bytes = new byte[byteSize];
		input.read(bytes);
		tdigest = AVLTreeDigest.fromBytes(ByteBuffer.wrap(bytes));
	}

	private Map<Double, Double> getCache() {
		if (cdfCache == null) {
			cdfCache = new LinkedHashMap<Double, Double>(
					MAX_CACHED_CDFS + 1,
					.75F,
					true) {
				private static final long serialVersionUID = 1L;

				@Override
				public boolean removeEldestEntry(
						final Map.Entry<Double, Double> eldest ) {
					return size() > MAX_CACHED_CDFS;
				}
			};
		}
		return cdfCache;
	}

	// cache results

	public double cdf(
			final double x ) {
		Map<Double, Double> cache = getCache();
		Double result = cache.get(x);
		if (result == null) {
			result = tdigest.cdf(x);
			cache.put(
					x,
					result);
		}
		return result;
	}
}
