package mil.nga.giat.geowave.process.nyctlc;

import java.io.Serializable;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils;

public class NYCTLCData implements
		Serializable
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private double weight;
	private double passengerCount;
	private double tripDistance;
	private double fareAmount;
	private double extra;
	private double mtaTax;
	private double tipAmount;
	private double tollsAmount;
	private double totalAmount;
	private double duration;
	private double tipPerDuration;
	private double farePerDuration;
	private double tipPerTripDistance;
	private double farePerTripDistance;

	protected NYCTLCData() {}

	public NYCTLCData(
			final double weight,
			final TDigestSerializable[] tdigests,
			final SimpleFeature f ) {
		super();
		this.weight = weight;
		final Number[] v = featureToValues(f);
		passengerCount = getValue(
				weight,
				v[0],
				tdigests[0]);
		tripDistance = getValue(
				weight,
				v[1],
				tdigests[1]);
		fareAmount = getValue(
				weight,
				v[2],
				tdigests[2]);
		extra = getValue(
				weight,
				v[3],
				tdigests[3]);
		mtaTax = getValue(
				weight,
				v[4],
				tdigests[4]);
		tipAmount = getValue(
				weight,
				v[5],
				tdigests[5]);
		tollsAmount = getValue(
				weight,
				v[6],
				tdigests[6]);
		totalAmount = getValue(
				weight,
				v[7],
				tdigests[7]);
		duration = getValue(
				weight,
				v[8],
				tdigests[8]);
		tipPerDuration = getValue(
				weight,
				v[9],
				tdigests[9]);
		farePerDuration = getValue(
				weight,
				v[10],
				tdigests[10]);
		tipPerTripDistance = getValue(
				weight,
				v[11],
				tdigests[11]);
		farePerTripDistance = getValue(
				weight,
				v[12],
				tdigests[12]);
	}

	public double getWeight() {
		return weight;
	}

	public static double getValue(
			final double weight,
			final Number v,
			final TDigestSerializable td ) {
		double cdf;
		final boolean isInteger = (!((v instanceof Float) || (v instanceof Double)));
		final double d = v.doubleValue();
		if (isInteger) {
			// its not continous, let's average
			// the cdf to avoid a problem with
			// too few unique values

			cdf = (td.cdf(d) + td.cdf(d - 1)) / 2;
		}
		else {
			cdf = td.cdf(d);
		}
		return weight * ((2 * cdf) - 1);
	}

	public static Number[] featureToValues(
			final SimpleFeature f ) {
		final Number[] v = new Number[14];
		v[0] = ((Number) f.getAttribute(NYCTLCUtils.Field.PASSENGER_COUNT.getIndexedName()));
		v[1] = ((Number) f.getAttribute(NYCTLCUtils.Field.TRIP_DISTANCE.getIndexedName()));
		v[2] = ((Number) f.getAttribute(NYCTLCUtils.Field.FARE_AMOUNT.getIndexedName()));
		v[3] = ((Number) f.getAttribute(NYCTLCUtils.Field.EXTRA.getIndexedName()));
		v[4] = ((Number) f.getAttribute(NYCTLCUtils.Field.MTA_TAX.getIndexedName()));
		v[5] = ((Number) f.getAttribute(NYCTLCUtils.Field.TIP_AMOUNT.getIndexedName()));
		v[6] = ((Number) f.getAttribute(NYCTLCUtils.Field.TOLLS_AMOUNT.getIndexedName()));
		v[7] = ((Number) f.getAttribute(NYCTLCUtils.Field.TOTAL_AMOUNT.getIndexedName()));
		v[8] = ((Number) f.getAttribute(NYCTLCUtils.Field.DURATION.getIndexedName()));
		v[9] = (v[8].doubleValue() > 0.0 ? v[5].doubleValue() / v[8].doubleValue() : 0.0);
		v[10] = (v[8].doubleValue() > 0.0 ? v[2].doubleValue() / v[8].doubleValue() : 0.0);
		v[11] = (v[1].doubleValue() > 0.0 ? v[5].doubleValue() / v[1].doubleValue() : 0.0);
		v[12] = (v[1].doubleValue() > 0.0 ? v[2].doubleValue() / v[1].doubleValue() : 0.0);
		return v;
	}

	public NYCTLCData add(
			final NYCTLCData other ) {
		weight += other.weight;

		duration += other.duration;
		extra += other.extra;
		fareAmount += other.fareAmount;
		farePerDuration += other.farePerDuration;
		farePerTripDistance += other.farePerTripDistance;
		mtaTax += other.mtaTax;

		passengerCount += other.passengerCount;
		tipAmount += other.tipAmount;
		tipPerDuration += other.tipPerDuration;
		tipPerTripDistance += other.tipPerTripDistance;
		tollsAmount += other.tollsAmount;
		totalAmount += other.totalAmount;
		tripDistance += other.tripDistance;

		return this;
	}

	public double getValue(
			final String typeName ) {
		switch (typeName) {
			case "base":
				return weight;
			case "passenger_count":
				return passengerCount;
			case "trip_distance":
				return tripDistance;
			case "fare_amount":
				return fareAmount;
			case "extra":
				return extra;
			case "mta_tax":
				return mtaTax;
			case "tip_amount":
				return tipAmount;
			case "tolls_amount":
				return tollsAmount;
			case "total_amount":
				return totalAmount;
			case "duration":
				return duration;
			case "tip_per_duration":
				return duration;
			case "fare_per_duration":
				return farePerDuration;
			case "tip_per_distance":
				return tipPerTripDistance;
			case "fare_per_distance":
				return farePerTripDistance;
		}
		return 0;
	}
}
