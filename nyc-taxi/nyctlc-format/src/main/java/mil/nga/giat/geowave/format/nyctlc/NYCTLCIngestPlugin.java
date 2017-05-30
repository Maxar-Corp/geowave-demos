package mil.nga.giat.geowave.format.nyctlc;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.adapter.vector.ingest.SimpleFeatureIngestOptions;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils.Field;
import mil.nga.giat.geowave.format.nyctlc.adapter.NYCTLCDataAdapter;
import mil.nga.giat.geowave.format.nyctlc.avro.NYCTLCEntry;
import mil.nga.giat.geowave.format.nyctlc.ingest.NYCTLCDimensionalityTypeProvider;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.mortbay.log.Log;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by geowave on 4/25/16.
 */
public class NYCTLCIngestPlugin extends
		AbstractSimpleFeatureIngestPlugin<NYCTLCEntry>
{

	private final static Logger LOGGER = Logger.getLogger(NYCTLCIngestPlugin.class);

	private final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private final static String DATE_START_FORMAT = "yyyyMMdd";

	private SimpleFeatureType pointType;
	private SimpleFeatureBuilder pointBuilder;
	private ByteArrayId pointKey;
	private boolean dropoff = false;
	private boolean timerange = false;

	public NYCTLCIngestPlugin() {
		this(
				null);
	}

	public NYCTLCIngestPlugin(
			NYCTLCOptionProvider options ) {

		if (options != null) {
			dropoff = options.isDropoff();
		}
		if (options != null) {
			timerange = options.isTimerange();
		}
		initType();
	}

	private void initType() {
		pointType = NYCTLCUtils.createPointDataType(
				dropoff,
				timerange);
		pointBuilder = new SimpleFeatureBuilder(
				pointType);
		pointKey = new ByteArrayId(
				StringUtils.stringToBinary(pointType.getTypeName()));
	}

	@Override
	protected WritableDataAdapter<SimpleFeature> newAdapter(
			SimpleFeatureType type,
			FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler ) {

		return new NYCTLCDataAdapter(
				type,
				fieldVisiblityHandler);
	}

	@Override
	protected SimpleFeatureType[] getTypes() {

		if (dropoff) {
			System.out.println("Using Drop-Off points");
		}
		else {
			System.out.println("Using Pick-Up points");
		}

		return new SimpleFeatureType[] {

			NYCTLCUtils.createPointDataType(
					dropoff,
					timerange)
		};
	}

	public SimpleFeature toGeoWaveDataInternalWrapper(
			final NYCTLCEntry point,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility ) {
		try (CloseableIterator<GeoWaveData<SimpleFeature>> it = toGeoWaveDataInternal(
				point,
				primaryIndexIds,
				globalVisibility)) {
			if (it.hasNext()) {
				return it.next().getValue();
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final NYCTLCEntry point,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility ) {

		final Collection<ByteArrayId> indexIds = new ArrayList<ByteArrayId>();
		if (primaryIndexIds != null && !primaryIndexIds.isEmpty()) indexIds.addAll(primaryIndexIds);
		for (final PrimaryIndex index : getRequiredIndices())
			indexIds.add(index.getId());

		final List<GeoWaveData<SimpleFeature>> featureData = new ArrayList<GeoWaveData<SimpleFeature>>();
		// for now ignore the default values because avro has already been
		// written for them when they are incorrectly parsed
		if (point.getPickupLatitude() != 0.0 && point.getPickupLongitude() != 0.0 && point.getDropoffLatitude() != 0.0
				&& point.getDropoffLongitude() != 0.0 && point.getTimeOfDaySec() != 0) {

			String uniqueId = "";

			for (final NYCTLCUtils.Field field : NYCTLCUtils.Field.values()) {
				switch (field) {
					case VENDOR_ID:
						pointBuilder.set(
								NYCTLCUtils.Field.VENDOR_ID.getIndexedName(),
								point.getVendorId());
						break;
					case PICKUP_DATETIME:
						pointBuilder.set(
								NYCTLCUtils.Field.PICKUP_DATETIME.getIndexedName(),
								new Date(
										point.getPickupDatetime()));
						uniqueId += point.getPickupDatetime() + "_";
						break;
					case DROPOFF_DATETIME:
						pointBuilder.set(
								NYCTLCUtils.Field.DROPOFF_DATETIME.getIndexedName(),
								new Date(
										point.getDropoffDatetime()));
						uniqueId += point.getDropoffDatetime() + "_";
						break;
					case PASSENGER_COUNT:
						pointBuilder.set(
								NYCTLCUtils.Field.PASSENGER_COUNT.getIndexedName(),
								point.getPassengerCount());
						break;
					case TRIP_DISTANCE:
						pointBuilder.set(
								NYCTLCUtils.Field.TRIP_DISTANCE.getIndexedName(),
								point.getTripDistance());
						break;
					case PICKUP_LONGITUDE:
						pointBuilder.set(
								NYCTLCUtils.Field.PICKUP_LONGITUDE.getIndexedName(),
								point.getPickupLongitude());
						break;
					case PICKUP_LATITUDE:
						pointBuilder.set(
								NYCTLCUtils.Field.PICKUP_LATITUDE.getIndexedName(),
								point.getPickupLatitude());
						break;
					case PICKUP_LOCATION:
						final Point pickupPoint = mil.nga.giat.geowave.core.geotime.GeometryUtils.GEOMETRY_FACTORY
								.createPoint(new Coordinate(
										point.getPickupLongitude(),
										point.getPickupLatitude()));
						pointBuilder.set(
								NYCTLCUtils.Field.PICKUP_LOCATION.getIndexedName(),
								pickupPoint);
						uniqueId += pickupPoint.toText() + "_";
						break;
					case STORE_AND_FWD_FLAG:
						pointBuilder.set(
								NYCTLCUtils.Field.STORE_AND_FWD_FLAG.getIndexedName(),
								point.getStoreAndFwdFlag());
						break;
					case RATE_CODE_ID:
						pointBuilder.set(
								NYCTLCUtils.Field.RATE_CODE_ID.getIndexedName(),
								point.getRateCodeId());
						break;
					case DROPOFF_LONGITUDE:
						pointBuilder.set(
								NYCTLCUtils.Field.DROPOFF_LONGITUDE.getIndexedName(),
								point.getDropoffLongitude());
						break;
					case DROPOFF_LATITUDE:
						pointBuilder.set(
								NYCTLCUtils.Field.DROPOFF_LATITUDE.getIndexedName(),
								point.getDropoffLatitude());
						break;
					case DROPOFF_LOCATION:
						final Point dropoffPoint = mil.nga.giat.geowave.core.geotime.GeometryUtils.GEOMETRY_FACTORY
								.createPoint(new Coordinate(
										point.getDropoffLongitude(),
										point.getDropoffLatitude()));
						pointBuilder.set(
								NYCTLCUtils.Field.DROPOFF_LOCATION.getIndexedName(),
								dropoffPoint);
						uniqueId += dropoffPoint.toText() + "_";
						break;
					case PAYMENT_TYPE:
						pointBuilder.set(
								NYCTLCUtils.Field.PAYMENT_TYPE.getIndexedName(),
								point.getPaymentType());
						break;
					case FARE_AMOUNT:
						pointBuilder.set(
								NYCTLCUtils.Field.FARE_AMOUNT.getIndexedName(),
								point.getFareAmount());
						break;
					case EXTRA:
						pointBuilder.set(
								NYCTLCUtils.Field.EXTRA.getIndexedName(),
								point.getExtra());
						break;
					case MTA_TAX:
						pointBuilder.set(
								NYCTLCUtils.Field.MTA_TAX.getIndexedName(),
								point.getMtaTax());
						break;
					case IMPROVEMENT_SURCHARGE:
						pointBuilder.set(
								NYCTLCUtils.Field.IMPROVEMENT_SURCHARGE.getIndexedName(),
								point.getImprovementSurcharge());
						break;
					case TIP_AMOUNT:
						pointBuilder.set(
								NYCTLCUtils.Field.TIP_AMOUNT.getIndexedName(),
								point.getTipAmount());
						break;
					case TOLLS_AMOUNT:
						pointBuilder.set(
								NYCTLCUtils.Field.TOLLS_AMOUNT.getIndexedName(),
								point.getTollsAmount());
						break;
					case TOTAL_AMOUNT:
						pointBuilder.set(
								NYCTLCUtils.Field.TOTAL_AMOUNT.getIndexedName(),
								point.getTotalAmount());
						break;
					case TRIP_TYPE:
						pointBuilder.set(
								NYCTLCUtils.Field.TRIP_TYPE.getIndexedName(),
								point.getTripType());
						break;
					case EHAIL_FEE:
						pointBuilder.set(
								NYCTLCUtils.Field.EHAIL_FEE.getIndexedName(),
								point.getEhailFee());
						break;
					case TIME_OF_DAY_SEC:
						pointBuilder.set(
								NYCTLCUtils.Field.TIME_OF_DAY_SEC.getIndexedName(),
								point.getTimeOfDaySec());
						break;
					case DAY_OF_WEEK:
						Calendar cal = Calendar.getInstance();
						cal.setTimeInMillis(point.getPickupDatetime());
						pointBuilder.set(
								NYCTLCUtils.Field.DAY_OF_WEEK.getIndexedName(),
								cal.get(Calendar.DAY_OF_WEEK));
						break;
					case DURATION:
						pointBuilder.set(
								NYCTLCUtils.Field.DURATION.getIndexedName(),
								point.getDropoffDatetime() - point.getPickupDatetime());
						break;
					case CAB_TYPE:
						pointBuilder.set(
								NYCTLCUtils.Field.CAB_TYPE.getIndexedName(),
								point.getCabType());
						break;
				}
			}

			featureData.add(new GeoWaveData<SimpleFeature>(
					pointKey,
					indexIds,
					pointBuilder.buildFeature(uniqueId)));

		}
		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(
				featureData.iterator());
	}

	@Override
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}

	@Override
	public IngestPluginBase<NYCTLCEntry, SimpleFeature> getIngestWithAvroPlugin() {
		return new IngestNYCTLCFromHdfs(
				this);
	}

	@Override
	public NYCTLCEntry[] toAvroObjects(
			File file ) {
		final SimpleDateFormat dateFormat = new SimpleDateFormat(
				DATE_FORMAT);
		final SimpleDateFormat dateStartFormat = new SimpleDateFormat(
				DATE_START_FORMAT);

		int cabType = 0;
		if (file.getName().toLowerCase().contains(
				"yellow"))
			cabType = 1;
		else if (file.getName().toLowerCase().contains(
				"green")) cabType = 2;

		BufferedReader fr = null;
		BufferedReader br = null;
		final List<NYCTLCEntry> pts = new ArrayList<NYCTLCEntry>();
		try {
			fr = new BufferedReader(
					new InputStreamReader(
							new FileInputStream(
									file),
							StringUtils.GEOWAVE_CHAR_SET));
			br = new BufferedReader(
					fr);
			String line;
			try {
				final String[] fields = br.readLine().split(
						",");
				while ((line = br.readLine()) != null) {

					if (line.isEmpty()) continue;

					final String[] vals = line.split(",");
					final NYCTLCEntry pt = new NYCTLCEntry();

					pt.setCabType(cabType);
					// Sets the cabType beforehand because there is no cabType
					// column in the table
					Set<Field> requiredFieldsMissing = new HashSet<Field>(
							NYCTLCUtils.REQUIRED_FIELDS);
					for (int fieldIdx = 0; fieldIdx < fields.length; fieldIdx++) {
						final NYCTLCUtils.Field field = NYCTLCUtils.Field.getField(fields[fieldIdx]);
						boolean success = true;
						if (field != null && fieldIdx < vals.length) {
							try {
								switch (field) {
									case VENDOR_ID:
										if (vals[fieldIdx].equals("1") || vals[fieldIdx].equals("2"))
											pt.setVendorId(Integer.parseInt(vals[fieldIdx]));
										else {
											if (vals[fieldIdx].toLowerCase().equals(
													"cmt")) {
												pt.setVendorId(1);
												requiredFieldsMissing.remove(field);
											}
											else if (vals[fieldIdx].toLowerCase().equals(
													"vts")) {
												pt.setVendorId(2);
												requiredFieldsMissing.remove(field);
											}
											else {
												success = false;
												pt.setVendorId(0); // unknown
											}
										}
										break;
									case PICKUP_DATETIME:
										try {
											final Date pickupDate = dateFormat.parse(vals[fieldIdx]);
											pt.setPickupDatetime(pickupDate.getTime());

											final Date pickupDateStart = dateStartFormat.parse(dateStartFormat
													.format(pickupDate));
											pt.setTimeOfDaySec(new Long(
													TimeUnit.MILLISECONDS.toSeconds(pickupDate.getTime()
															- pickupDateStart.getTime())).longValue());

										}
										catch (ParseException e) {
											success = false;
											Log.warn(
													"Error parsing pickup datetime: " + vals[fieldIdx],
													e);
										}
										break;
									case DROPOFF_DATETIME:
										try {
											pt.setDropoffDatetime(dateFormat.parse(
													vals[fieldIdx]).getTime());
										}
										catch (ParseException e) {
											success = false;
											Log.warn(
													"Error parsing dropoff datetime: " + vals[fieldIdx],
													e);
										}
										break;
									case PASSENGER_COUNT:
										pt.setPassengerCount(Integer.parseInt(vals[fieldIdx]));
										break;
									case TRIP_DISTANCE:
										pt.setTripDistance(Double.parseDouble(vals[fieldIdx]));
										break;
									case PICKUP_LONGITUDE:
										double longitude = Double.parseDouble(vals[fieldIdx]);
										if (longitude > NYCTLCDimensionalityTypeProvider.MAX_LON
												|| longitude < NYCTLCDimensionalityTypeProvider.MIN_LON) {
											success = false;
											break;
										}
										pt.setPickupLongitude(longitude);
										break;
									case PICKUP_LATITUDE:
										double latitude = Double.parseDouble(vals[fieldIdx]);
										if (latitude > NYCTLCDimensionalityTypeProvider.MAX_LAT
												|| latitude < NYCTLCDimensionalityTypeProvider.MIN_LAT) {
											success = false;
											break;
										}
										pt.setPickupLatitude(latitude);
										break;
									case PICKUP_LOCATION:
										// do nothing
										break;
									case STORE_AND_FWD_FLAG:
										String strFwdFlg = vals[fieldIdx].toLowerCase();
										pt.setStoreAndFwdFlag(strFwdFlg.equals("1") || strFwdFlg.equals("y"));
										break;
									case RATE_CODE_ID:
										pt.setRateCodeId(Integer.parseInt(vals[fieldIdx]));
										break;
									case DROPOFF_LONGITUDE:
										double dropOffLongitude = Double.parseDouble(vals[fieldIdx]);
										if (dropOffLongitude > NYCTLCDimensionalityTypeProvider.MAX_LON
												|| dropOffLongitude < NYCTLCDimensionalityTypeProvider.MIN_LON) {
											success = false;
											break;
										}
										pt.setDropoffLongitude(dropOffLongitude);
										break;
									case DROPOFF_LATITUDE:
										double dropOffLatitude = Double.parseDouble(vals[fieldIdx]);
										if (dropOffLatitude > NYCTLCDimensionalityTypeProvider.MAX_LAT
												|| dropOffLatitude < NYCTLCDimensionalityTypeProvider.MIN_LAT) {
											success = false;
											break;
										}
										pt.setDropoffLatitude(dropOffLatitude);
										break;
									case DROPOFF_LOCATION:
										// do nothing
										break;
									case PAYMENT_TYPE:
										String pmntType = vals[fieldIdx].toLowerCase().replace(
												"_",
												"").replace(
												" ",
												"");
										if (NumberUtils.isNumber(pmntType)) {
											pt.setPaymentType(Integer.parseInt(vals[fieldIdx]));
										}
										else {
											if (pmntType.contains("crd") || pmntType.contains("cre"))
												pt.setPaymentType(1);
											else if (pmntType.contains("cas") || pmntType.contains("csh"))
												pt.setPaymentType(2);
											else if (pmntType.contains("nocharge") || pmntType.equals("no")
													|| pmntType.equals("na") || pmntType.equals("noc"))
												pt.setPaymentType(3);
											else if (pmntType.contains("dis"))
												pt.setPaymentType(4);
											else if (pmntType.contains("unk"))
												pt.setPaymentType(5);
											else if (pmntType.contains("void"))
												pt.setPaymentType(6);
											else {
												success = false;
												LOGGER.warn("Unknown payment type [" + pmntType + "]");
												pt.setPaymentType(0);
											}
										}
										break;
									case FARE_AMOUNT:
										pt.setFareAmount(Double.parseDouble(vals[fieldIdx]));
										break;
									case EXTRA:
										pt.setExtra(Double.parseDouble(vals[fieldIdx]));
										break;
									case MTA_TAX:
										pt.setMtaTax(Double.parseDouble(vals[fieldIdx]));
										break;
									case IMPROVEMENT_SURCHARGE:
										pt.setImprovementSurcharge(Double.parseDouble(vals[fieldIdx]));
										break;
									case TIP_AMOUNT:
										pt.setTipAmount(Double.parseDouble(vals[fieldIdx]));
										break;
									case TOLLS_AMOUNT:
										pt.setTollsAmount(Double.parseDouble(vals[fieldIdx]));
										break;
									case TOTAL_AMOUNT:
										pt.setTotalAmount(Double.parseDouble(vals[fieldIdx]));
										break;
									case TRIP_TYPE:
										pt.setTripType(Integer.parseInt(vals[fieldIdx]));
										break;
									case EHAIL_FEE:
										pt.setEhailFee(Double.parseDouble(vals[fieldIdx]));
										break;
									case CAB_TYPE:
										pt.setCabType(cabType);
										break;
								}

								if (success) {
									requiredFieldsMissing.remove(field);
								}
							}
							catch (NumberFormatException | NullPointerException e) {
								if (!vals[fieldIdx].isEmpty())
									LOGGER.warn("Unable to parse field [" + fields[fieldIdx] + "] with value ["
											+ vals[fieldIdx] + "]");
								else
									LOGGER.info("Field [" + fields[fieldIdx] + "] has no value");
							}
						}
						else {
							if (field == null)
								LOGGER.warn("Unknown field encountered: " + fields[fieldIdx]);
							else if (fieldIdx >= vals.length && !field.equals(NYCTLCUtils.Field.TRIP_TYPE))
								LOGGER.warn("Field not present in row: " + fields[fieldIdx]);
						}
					}
					if (requiredFieldsMissing.isEmpty()) {
						pts.add(pt);
					}

					if (pts.size() % 10000 == 0)
						LOGGER.warn(pts.size() + " entries serialized to avro from [" + file.getName() + "].");
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Error reading line from file: " + file.getName(),
						e);
			}
		}
		catch (final FileNotFoundException e) {
			LOGGER.warn(
					"Error parsing csv file: " + file.getName(),
					e);
		}
		finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(fr);
		}
		return pts.toArray(new NYCTLCEntry[pts.size()]);
	}

	@Override
	public byte[] toBinary() {
		byte[] parentBytes = super.toBinary();
		ByteBuffer buf = ByteBuffer.allocate(parentBytes.length + 6);
		buf.putInt(parentBytes.length);
		buf.put(parentBytes);
		buf.put(dropoff ? (byte) 1 : (byte) 0);
		buf.put(timerange ? (byte) 1 : (byte) 0);
		return buf.array();
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		int length = buf.getInt();
		byte[] dst = new byte[length];
		buf.get(dst);
		dropoff = buf.get() == 1;
		timerange = buf.get() == 1;
		super.fromBinary(dst);
		initType();
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<NYCTLCEntry, SimpleFeature> ingestWithMapper() {
		return new IngestNYCTLCFromHdfs(
				this);
	}

	@Override
	public IngestWithReducer<NYCTLCEntry, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"NYCTLC cannot be ingested with a reducer");
	}

	@Override
	public Schema getAvroSchema() {
		return NYCTLCEntry.getClassSchema();
	}

	@Override
	public PrimaryIndex[] getRequiredIndices() {
		return new PrimaryIndex[] {};
	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"csv"
		};
	}

	@Override
	public void init(
			File baseDirectory ) {

	}

	@Override
	public boolean supportsFile(
			File file ) {
		return NYCTLCUtils.validate(file);
	}

	public static class IngestNYCTLCFromHdfs extends
			AbstractIngestSimpleFeatureWithMapper<NYCTLCEntry>
	{
		public IngestNYCTLCFromHdfs() {
			this(
					new NYCTLCIngestPlugin());
		}

		public IngestNYCTLCFromHdfs(
				final NYCTLCIngestPlugin parentPlugin ) {
			super(
					parentPlugin);
		}
	}
}
