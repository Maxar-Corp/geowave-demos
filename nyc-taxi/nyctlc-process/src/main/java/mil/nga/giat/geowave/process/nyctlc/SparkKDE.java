package mil.nga.giat.geowave.process.nyctlc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;

import com.beust.jcommander.ParameterException;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.analytic.mapreduce.kde.CellCounter;
import mil.nga.giat.geowave.analytic.mapreduce.kde.GaussianFilter;
import mil.nga.giat.geowave.analytics.spark.GeoWaveContext;
import mil.nga.giat.geowave.analytics.spark.GeoWaveRDD;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCIngestPlugin;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;

public class SparkKDE
{
	public static final int TILE_SCALE = 8;
	public static final int TILE_SIZE = 1 << TILE_SCALE;

	public static void main(
			final String[] args )
			throws CQLException,
			FactoryException {
		DataStorePluginOptions inputStoreOptions = null;
		// Attempt to load input store.
		if (inputStoreOptions == null) {
			final StoreLoader inputStoreLoader = new StoreLoader(
					args[0]);
			if (!inputStoreLoader.loadFromConfig(
					ConfigOptions.getDefaultPropertyFile())) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		;
		// Load the Indexes
		final IndexLoader indexLoader = new IndexLoader(
				args[1]);
		if (!indexLoader.loadFromConfig(
				ConfigOptions.getDefaultPropertyFile())) {
			throw new ParameterException(
					"Cannot find index(s) by name: " + args[2]);
		}

		final List<IndexPluginOptions> inputIndexOptions = indexLoader.getLoadedIndexes();
		final PrimaryIndex index = inputIndexOptions.get(
				0).createPrimaryIndex();
		final WritableDataAdapter a = new NYCTLCIngestPlugin().getDataAdapters(
				"")[0];
		final JavaPairRDD<GeoWaveInputKey, SimpleFeature> geowaveRdd = JavaPairRDD.fromRDD(
				GeoWaveRDD.rddForSimpleFeatures(
						new SparkContext(),
						new QueryOptions(),
						256,
						256,
						args.length > 4 ? (DistributableQuery) CQLQuery.createOptimalQuery(
								args[4],
								(GeotoolsFeatureDataAdapter) a,
								index) : null,
						GeoWaveContext.apply(
								inputStoreOptions,
								inputStoreOptions.getType(),
								inputStoreOptions.getGeowaveNamespace())),
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(
						GeoWaveInputKey.class),
				(ClassTag) scala.reflect.ClassTag$.MODULE$.apply(
						SimpleFeature.class));
		final int level = Integer.parseInt(
				args[2]);
		final int numXPosts = (int) Math.pow(
				2,
				level + 1);
		final int numYPosts = (int) Math.pow(
				2,
				level);
		final Function<Double, Double> identity = x -> x;

		final Function2<Double, Double, Double> sum = (
				final Double x,
				final Double y ) -> {
			return x + y;
		};
		geowaveRdd
				.flatMapToPair(
						new GeoWaveCellMapper(
								numXPosts,
								numYPosts))
				.combineByKey(
						identity,
						sum,
						sum)
				.mapToPair(
						new PairFunction<Tuple2<Long, Double>, Double, Long>() {
							/**
							 *
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<Double, Long> call(
									final Tuple2<Long, Double> item )
									throws Exception {
								return item.swap();
							}

						})
				.sortByKey()
				.repartition(
						1)
				.saveAsTextFile(
						args[3],
						GzipCodec.class);
	}

	public static long fromCellIndexToTileIndex(
			final long cellIndex,
			final int numXPosts,
			final int numYPosts ) {
		final int xPost = (int) (cellIndex / numYPosts);
		final int yPost = (int) (cellIndex % numYPosts);
		final int numXTiles = numXPosts / TILE_SIZE;
		final int numYTiles = numXPosts / TILE_SIZE;
		final int xTile = xPost / TILE_SIZE;
		final int yTile = yPost / TILE_SIZE;
		return GaussianFilter.getPosition(
				new int[] {
					xTile,
					yTile
				},
				new int[] {
					numXTiles,
					numYTiles
				});
	}

	public static Tuple3<Integer, Integer, Integer> getTileNumberFromTileIndex(
			final long tileIndex,
			final int zoom,
			final int numXTiles,
			final int numYTiles ) {
		final int xTile = (int) (tileIndex / numXTiles);
		final int yTile = (int) (tileIndex % numYTiles);

		final double lon = (((xTile + 0.5) * 360.0) / numXTiles) - 180.0;
		final double lat = (((yTile + 0.5) * 180.0) / numYTiles) - 90.0;
		int xtile = (int) Math.floor(((lon + 180) / 360) * (1 << zoom));
		int ytile = (int) Math.floor(((1 - (Math.log(Math.tan(Math.toRadians(lat))
				+ (1 / Math.cos(Math.toRadians(lat)))) / Math.PI)) / 2)
				* (1 << zoom));
		if (xtile < 0) {
			xtile = 0;
		}
		if (xtile >= (1 << zoom)) {
			xtile = ((1 << zoom) - 1);
		}
		if (ytile < 0) {
			ytile = 0;
		}
		if (ytile >= (1 << zoom)) {
			ytile = ((1 << zoom) - 1);
		}
		return new Tuple3<>(
				zoom,
				xtile,
				ytile);
	}

	public static TileBounds tile2boundingBox(
			final int x,
			final int y,
			final int zoom ) {
		final double north = tile2lat(
				y,
				zoom);
		final double south = tile2lat(
				y + 1,
				zoom);
		final double west = tile2lon(
				x,
				zoom);
		final double east = tile2lon(
				x + 1,
				zoom);
		return new TileBounds(
				west,
				east,
				south,
				north);
	}

	static double tile2lon(
			final int x,
			final int z ) {
		return ((x / Math.pow(
				2.0,
				z)) * 360.0) - 180;
	}

	static double tile2lat(
			final int y,
			final int z ) {
		final double n = Math.PI - ((2.0 * Math.PI * y) / Math.pow(
				2.0,
				z));
		return Math.toDegrees(Math.atan(Math.sinh(n)));
	}

	public static Tuple2<Double, Double> cellToLatLon(
			final long cellIndex,
			final int numXPosts,
			final int numYPosts ) {

		final int xPost = (int) (cellIndex / numYPosts);
		final int yPost = (int) (cellIndex % numYPosts);

		final double lon = (((xPost + 0.5) * 360.0) / numXPosts) - 180.0;
		final double lat = (((yPost + 0.5) * 180.0) / numYPosts) - 90.0;
		return new Tuple2<Double, Double>(
				lon,
				lat);
	}

	public static Tuple3<Integer, Integer, Integer> tmsCellToTile(
			final long cellIndex,
			final int zoom ) {
		final int numPosts = 1 << (zoom + SparkKDE.TILE_SCALE);
		final int xPost = (int) (cellIndex / numPosts);
		final int yPost = (int) (cellIndex % numPosts);
		return new Tuple3<Integer, Integer, Integer>(
				zoom,
				xPost / TILE_SIZE,
				yPost / TILE_SIZE);
	}

	public static Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> tmsCellToTileAndPosition(
			final long cellIndex,
			final int zoom ) {
		final int numPosts = 1 << (zoom + SparkKDE.TILE_SCALE);
		final int xPost = (int) (cellIndex / numPosts);
		final int yPost = (int) (cellIndex % numPosts);

		return new Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>(
				new Tuple3<Integer, Integer, Integer>(
						zoom,
						xPost / TILE_SIZE,
						yPost / TILE_SIZE),
				new Tuple2<Integer, Integer>(
						xPost % TILE_SIZE,
						yPost % TILE_SIZE));
	}

	public static Tuple2<Integer, Integer> interpolatePosition(
			final long cellIndex,
			final int numXPosts,
			final int numYPosts,
			final TileBounds bounds ) {
		final Tuple2<Double, Double> lonLat = cellToLatLon(
				cellIndex,
				numXPosts,
				numYPosts);
		final double pctLon = (lonLat._1 - bounds.tileWestLon) / bounds.deltaLon;
		final double pctLat = (lonLat._2 - bounds.tileSouthLat) / bounds.deltaLat;
		return new Tuple2<Integer, Integer>(
				Math.min(
						(int) Math.round(pctLon * TILE_SIZE),
						TILE_SIZE - 1),
				Math.max(
						(TILE_SIZE - (int) Math.round(pctLat * TILE_SIZE) - 1),
						0));
	}

	public static Tuple3<Integer, Integer, Integer> getTileNumberFromCellIndex(
			final long cellIndex,
			final int numXPosts,
			final int numYPosts,
			final int zoom ) {
		final Tuple2<Double, Double> lonLat = cellToLatLon(
				cellIndex,
				numXPosts,
				numYPosts);
		return getTileNumberFromLonLat(
				lonLat,
				zoom);
	}

	public static Tuple3<Integer, Integer, Integer> getTileNumberFromLonLat(
			final Tuple2<Double, Double> lonLat,
			final int zoom ) {
		final Tuple2<Double, Double> coords = getTileCoordsFromLonLat(
				lonLat,
				zoom);
		int xtile = (int) Math.floor(coords._1);
		int ytile = (int) Math.floor(coords._2);
		if (xtile < 0) {
			xtile = 0;
		}
		if (xtile >= (1 << zoom)) {
			xtile = ((1 << zoom) - 1);
		}
		if (ytile < 0) {
			ytile = 0;
		}
		if (ytile >= (1 << zoom)) {
			ytile = ((1 << zoom) - 1);
		}
		return new Tuple3<>(
				zoom,
				xtile,
				ytile);
	}

	public static Tuple2<Double, Double> getTileCoordsFromLonLat(
			final Tuple2<Double, Double> lonLat,
			final int zoom ) {
		final double xtile = ((lonLat._1 + 180) / 360) * (1 << zoom);
		final double ytile = ((1 - (Math.log(Math.tan(Math.toRadians(lonLat._2))
				+ (1 / Math.cos(Math.toRadians(lonLat._2)))) / Math.PI)) / 2)
				* (1 << zoom);
		return new Tuple2<Double, Double>(
				xtile,
				ytile);
	}

	public static Tuple2<Integer, Integer> fromCellIndexToXY(
			final long cellIndex,
			final int numXPosts,
			final int numYPosts,
			final int numXTiles,
			final int numYTiles ) {
		final int xPost = (int) (cellIndex / numYPosts);
		final int yPost = (int) (cellIndex % numYPosts);
		final int x = (xPost % TILE_SIZE);
		final int y = (yPost % TILE_SIZE);
		return new Tuple2<Integer, Integer>(
				x,
				TILE_SIZE - y - 1); // remember java rasters go
									// from 0 at the
		// top
		// to (height-1) at the bottom, so we have
		// to
		// inverse the y here which goes from bottom
		// to top
	}

	protected static class GeoWaveCellMapper implements
			org.apache.spark.api.java.function.PairFlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>, Long, Double>
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 1L;
		private final int numXPosts;
		private final int numYPosts;

		public GeoWaveCellMapper(
				final int numXPosts,
				final int numYPosts ) {
			this.numXPosts = numXPosts;
			this.numYPosts = numYPosts;
		}

		@Override
		public Iterator<Tuple2<Long, Double>> call(
				final Tuple2<GeoWaveInputKey, SimpleFeature> t )
				throws Exception {
			return getCells(
					t._2,
					numXPosts,
					numYPosts);
		}
	}

	public static Iterator<Tuple2<Long, Double>> getCells(
			final SimpleFeature s,
			final int numXPosts,
			final int numYPosts ) {

		final List<Tuple2<Long, Double>> cells = new ArrayList<>();

		Point pt = null;
		if (s != null) {
			final Object geomObj = s.getDefaultGeometry();
			if ((geomObj != null) && (geomObj instanceof Geometry)) {
				pt = ((Geometry) geomObj).getCentroid();
				GaussianFilter.incrementPtFast(
						pt.getY(),
						pt.getX(),
						new CellCounter() {

							@Override
							public void increment(
									final long cellId,
									final double weight ) {
								cells.add(new Tuple2<Long, Double>(
										cellId,
										weight));

							}
						},
						numXPosts,
						numYPosts);
			}
		}
		return cells.iterator();
	}

	public static Iterator<Tuple2<Long, Double>> getTMSCells(
			final SimpleFeature s,
			final boolean useDropoff,
			final int zoom ) {
		final List<Tuple2<Long, Double>> cells = new ArrayList<>();
		final int numPosts = 1 << (zoom + TILE_SCALE);
		Point pt = null;
		if (s != null) {
			final Object geomObj;
			if (useDropoff) {
				geomObj = s.getAttribute(NYCTLCUtils.Field.DROPOFF_LOCATION.getIndexedName());
			}
			else {
				geomObj = s.getDefaultGeometry();
			}
			if ((geomObj != null) && (geomObj instanceof Geometry)) {
				pt = ((Geometry) geomObj).getCentroid();
				final Tuple2<Double, Double> coords = getTileCoordsFromLonLat(
						new Tuple2<Double, Double>(
								pt.getX(),
								pt.getY()),
						zoom + TILE_SCALE);
				GaussianFilter.incrementPtFast(
						new double[] {
							coords._1,
							coords._2
						},
						new int[] {
							numPosts,
							numPosts,
						},
						new CellCounter() {
							@Override
							public void increment(
									final long cellId,
									final double weight ) {
								cells.add(new Tuple2<Long, Double>(
										cellId,
										weight));
							}
						});
			}
		}
		return cells.iterator();
	}

	public static Iterator<Tuple2<Long, Tuple2<Double, Double>>> getTMSCellsWithAttribute(
			final SimpleFeature s,
			final boolean useDropoff,
			final int zoom,
			final String attributeName ) {
		final List<Tuple2<Long, Tuple2<Double, Double>>> cells = new ArrayList<>();
		final int numPosts = 1 << (zoom + TILE_SCALE);
		Point pt = null;
		if (s != null) {
			final Object geomObj;
			if (useDropoff) {
				geomObj = s.getAttribute(NYCTLCUtils.Field.DROPOFF_LOCATION.getIndexedName());
			}
			else {
				geomObj = s.getDefaultGeometry();
			}
			if ((geomObj != null) && (geomObj instanceof Geometry)) {
				pt = ((Geometry) geomObj).getCentroid();
				final Tuple2<Double, Double> coords = getTileCoordsFromLonLat(
						new Tuple2<Double, Double>(
								pt.getX(),
								pt.getY()),
						zoom + TILE_SCALE);
				final double attrValue = ((Number) s.getAttribute(attributeName)).doubleValue();
				GaussianFilter.incrementPtFast(
						new double[] {
							coords._1,
							coords._2
						},
						new int[] {
							numPosts,
							numPosts,
						},
						new CellCounter() {
							@Override
							public void increment(
									final long cellId,
									final double weight ) {
								cells.add(new Tuple2<Long, Tuple2<Double, Double>>(
										cellId,
										new Tuple2<Double, Double>(
												weight,
												weight * attrValue)));
							}
						});
			}
		}
		return cells.iterator();
	}

	protected static final class TileBounds
	{
		private final double tileWestLon;
		private final double tileEastLon;
		private final double tileSouthLat;
		private final double tileNorthLat;

		private final double deltaLon;
		private final double deltaLat;

		public TileBounds(
				final double tileWestLon,
				final double tileEastLon,
				final double tileSouthLat,
				final double tileNorthLat ) {
			this.tileWestLon = tileWestLon;
			this.tileEastLon = tileEastLon;
			this.tileSouthLat = tileSouthLat;
			this.tileNorthLat = tileNorthLat;
			deltaLon = tileEastLon - tileWestLon;
			deltaLat = tileNorthLat - tileSouthLat;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			long temp;
			temp = Double.doubleToLongBits(tileEastLon);
			result = (prime * result) + (int) (temp ^ (temp >>> 32));
			temp = Double.doubleToLongBits(tileNorthLat);
			result = (prime * result) + (int) (temp ^ (temp >>> 32));
			temp = Double.doubleToLongBits(tileSouthLat);
			result = (prime * result) + (int) (temp ^ (temp >>> 32));
			temp = Double.doubleToLongBits(tileWestLon);
			result = (prime * result) + (int) (temp ^ (temp >>> 32));
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final TileBounds other = (TileBounds) obj;
			if (Double.doubleToLongBits(tileEastLon) != Double.doubleToLongBits(other.tileEastLon)) {
				return false;
			}
			if (Double.doubleToLongBits(tileNorthLat) != Double.doubleToLongBits(other.tileNorthLat)) {
				return false;
			}
			if (Double.doubleToLongBits(tileSouthLat) != Double.doubleToLongBits(other.tileSouthLat)) {
				return false;
			}
			if (Double.doubleToLongBits(tileWestLon) != Double.doubleToLongBits(other.tileWestLon)) {
				return false;
			}
			return true;
		}
	}
}
