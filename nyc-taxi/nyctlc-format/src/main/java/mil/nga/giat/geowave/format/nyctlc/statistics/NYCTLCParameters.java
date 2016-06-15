package mil.nga.giat.geowave.format.nyctlc.statistics;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils.Field;

public class NYCTLCParameters implements
		Persistable
{
	private List<Integer> ordinals = new ArrayList<Integer>();

	public NYCTLCParameters() {
		super();
	}

	public void addField(
			Field field ) {
		if (field.getStatBuilder() != null) {
			ordinals.add(field.ordinal());
		}
	}

	public NYCTLCParameters(
			List<Integer> ordinals ) {
		super();
		this.ordinals = ordinals;
	}

	public List<Integer> getOrdinals() {
		return ordinals;
	}

	@Override
	public byte[] toBinary() {
		ByteBuffer buf = ByteBuffer.allocate(4 + 4 * ordinals.size());
		buf.putInt(ordinals.size());
		for (Integer ordinal : ordinals) {
			buf.putInt(ordinal);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		int size = buf.getInt();
		ordinals = new ArrayList<Integer>();
		for (int i = 0; i < size; i++) {
			ordinals.add(buf.getInt());
		}
	}

}
