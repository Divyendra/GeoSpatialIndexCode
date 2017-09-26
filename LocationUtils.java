package utility;


import com.google.common.geometry.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Divyu on 9/2/2015.
 */
public class LocationUtils {
    public static  final int HASHKEY_LEVEL = 12; //An approx cell area of 5.1 sq.km.
    //Near by cells of same level in S2 will have difference of '2' in their cellIds
    private static  final long MERGEABLECELLDIFFTHRESHOLD = 2;

    //Generates Geohash using S2library and returns it.
    //How it works resources: https://docs.google.com/presentation/d/1Hl4KapfAENAOf4gv-pSngKwvS_jwNVHRPZTTDzXXn6Q/view#slide=id.i0,
    //http://blog.christianperone.com/2015/08/googles-s2-geometry-on-the-sphere-cells-and-hilbert-curve/
    public static long generateCellId(double lat, double lon) {
        S2LatLng latLng = S2LatLng.fromDegrees(lat, lon);
//        S2Cell cell = new S2Cell(latLng);   //Try merging these two statements for : S2CellId.fromLatLng(latLng)
//        S2CellId cellId = cell.id();
        S2CellId cellId = S2CellId.fromLatLng(latLng);
        return cellId.id();
    }

    //Same as above. Returns S2CellId instead of long
    public static S2CellId generateCellIdAsCell(double lat, double lon) {
        S2LatLng latLng = S2LatLng.fromDegrees(lat, lon);
        S2CellId cellId = S2CellId.fromLatLng(latLng);
        return cellId;
    }

    public static long generateHashKey(S2CellId s2CellId) {
        return s2CellId.parent(HASHKEY_LEVEL).id();
    }

    public static long generateHashKey(double location_lat, double location_long) {
        S2LatLng latLng = S2LatLng.fromDegrees(location_lat, location_long);
//        S2Cell cell = new S2Cell(latLng);   //Try merging these two statements for : S2CellId.fromLatLng(latLng)
//        S2CellId cellId = cell.id();
        S2CellId cellId = S2CellId.fromLatLng(latLng);
        return cellId.parent(HASHKEY_LEVEL).id();
    }

    private static final int DEST_LOCALITY_LEVEL = 13; //Approx 1.25 sq.km area
    public static S2CellId getDestinationCell(double destLat, double destLong) {
        return generateCellIdAsCell(destLat, destLong).parent(DEST_LOCALITY_LEVEL);
    }

    public static List<S2CellId> getNeighBourCellIdsofDest(S2CellId destCellId) {
        List<S2CellId> neighbourCells = new ArrayList<>();
        destCellId.getAllNeighbors(destCellId.level(), neighbourCells);
        return neighbourCells;
    }

    public static boolean isMatchesList(List<S2CellId> s2CellIds, long whatToMatch) {
        if (whatToMatch == 0) return false;
        for (S2CellId id: s2CellIds) {
            if (id.id() == whatToMatch) return true;
        }
        return false;
    }

    public static HashMap<Long, List<GeohashRange>> getCellIdRangesSurrounding(double centerLat, double centerLong, S2LatLng centerLatLng, double radiusInMeter) {
        S2LatLngRect interestedRect = getBoundingLatLongRect(centerLat, centerLong, centerLatLng, radiusInMeter);
        S2CellUnion cellUnion = S2Manager.findCellIds(interestedRect);
        return mergeCells(cellUnion);
    }

    //Converts the circle of interest to S2LatLongRect. This code is inspired an mostly taken from 'DynamoGeo' library
    private static S2LatLngRect getBoundingLatLongRect(double centerLat, double centerLong, S2LatLng centerLatLng, double radiusInMeter) {
        double latReferenceUnit = centerLat > 0.0 ? -1.0 : 1.0;
        S2LatLng latReferenceLatLng = S2LatLng.fromDegrees(centerLat + latReferenceUnit,
                centerLong);
        double lngReferenceUnit = centerLong > 0.0 ? -1.0 : 1.0;
        S2LatLng lngReferenceLatLng = S2LatLng.fromDegrees(centerLat, centerLong
                + lngReferenceUnit);

        double latForRadius = radiusInMeter / centerLatLng.getEarthDistance(latReferenceLatLng);
        double lngForRadius = radiusInMeter / centerLatLng.getEarthDistance(lngReferenceLatLng);

        S2LatLng minLatLng = S2LatLng.fromDegrees(centerLat - latForRadius,
                centerLong - lngForRadius);
        S2LatLng maxLatLng = S2LatLng.fromDegrees(centerLat + latForRadius,
                centerLong + lngForRadius);

        return new S2LatLngRect(minLatLng, maxLatLng);
    }

    /**
     * Merge continuous cells in cellUnion and return a list of merged GeohashRanges.
     * @param cellUnion
     *            Container for multiple cells.
     * @return A list of merged GeohashRanges.
     */
    private static HashMap<Long, List<GeohashRange>> mergeCells(S2CellUnion cellUnion) {

        HashMap<Long, List<GeohashRange>> rangesSearchable = new HashMap<>();

        List<GeohashRange> ranges = new ArrayList<GeohashRange>();
        for (S2CellId c : cellUnion.cellIds()) {
            if (c.rangeMin().parent(HASHKEY_LEVEL).id() != c.parent(HASHKEY_LEVEL).id()) {
                System.out.println("Error in Merging: " + c.id()+ "(" + c.level() + ")" + "=" + c.rangeMin().id() + "->" + c.rangeMax().id());
                System.out.println(c.rangeMin().parent(HASHKEY_LEVEL).id() + "!=" + c.parent(HASHKEY_LEVEL).id());
            }
            GeohashRange range = new GeohashRange(c.rangeMin().id(), c.rangeMax().id(), c.parent(HASHKEY_LEVEL).id());
            boolean wasMerged = false;
            for (GeohashRange r : ranges) {
                if (r.tryMerge(range, MERGEABLECELLDIFFTHRESHOLD)) {
                    wasMerged = true;
                    break;
                }
            }
            if (!wasMerged) {
                ranges.add(range);
            }
        }

        for (GeohashRange iRange : ranges) {
            if (rangesSearchable.containsKey(iRange.getParentCellId())) {
                rangesSearchable.get(iRange.getParentCellId()).add(iRange);
            } else {
                List<GeohashRange> rangeList = new ArrayList<GeohashRange>();
                rangeList.add(iRange);
                rangesSearchable.put(iRange.getParentCellId(), rangeList);
            }
        }

        return rangesSearchable;
    }



}
