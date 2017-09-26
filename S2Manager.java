/*
 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 * http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package utility;

import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLngRect;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

//This class is taken from DynamoGeo project. Few optimisations are done over it. The main interested method in this class is 'findCellIds(..)'

public class S2Manager {

	//Our Query radius wil be near 1.5 km => 9 sq.km => so choose level 12 which has 5 sq.km(nearest one)
	private static int QUERY_APPR_LEVEL = 12;

	//Returns the cellids which lie inside the given S2LatLngRect
	public static S2CellUnion findCellIds(S2LatLngRect latLngRect) {

		ConcurrentLinkedQueue<S2CellId> queue = new ConcurrentLinkedQueue<S2CellId>();
		ArrayList<S2CellId> cellIds = new ArrayList<S2CellId>();

		for (S2CellId c = S2CellId.begin(0); !c.equals(S2CellId.end(0)); c = c.next()) {
			if (containsGeodataToFind(c, latLngRect)) {
				queue.add(c);
			}
		}

		processQueue(queue, cellIds, latLngRect);
		assert queue.size() == 0;
		queue = null;

		if (cellIds.size() > 0) {
			S2CellUnion cellUnion = new S2CellUnion();
			cellUnion.initFromCellIds(cellIds); // This normalize the cells.
			// cellUnion.initRawCellIds(cellIds); // This does not normalize the cells.
			cellIds = null;

			return cellUnion;
		}

		return null;
	}

	private static boolean containsGeodataToFind(S2CellId c, S2LatLngRect latLngRect) {
		if (latLngRect != null) {
			return latLngRect.intersects(new S2Cell(c));
		}

		return false;
	}

	private static void processQueue(ConcurrentLinkedQueue<S2CellId> queue, ArrayList<S2CellId> cellIds,
			S2LatLngRect latLngRect) {
		for (S2CellId c = queue.poll(); c != null; c = queue.poll()) {

			if (!c.isValid()) {
				break;
			}

			processChildren(c, latLngRect, queue, cellIds);
		}
	}

	private static void processChildren(S2CellId parent, S2LatLngRect latLngRect,
			ConcurrentLinkedQueue<S2CellId> queue, ArrayList<S2CellId> cellIds) {
		List<S2CellId> children = new ArrayList<S2CellId>(4);

		for (S2CellId c = parent.childBegin(); !c.equals(parent.childEnd()); c = c.next()) {
			if (containsGeodataToFind(c, latLngRect)) {
				children.add(c);
			}
		}

		/*
		 * The belwo strategy is taken from Dynamogeo library but few optimisations were done.
		 * 
		 * Current strategy:
		 * 1 or 2 cells contain cellIdToFind: Traverse the children of the cell.
		 * 3 cells contain cellIdToFind: Add 3 cells for result.
		 * 4 cells contain cellIdToFind: Add the parent for result if the level is less than QUERY_APPR_LEVEL
		 * 
		 * ** All non-leaf cells contain 4 child cells.
		 */
		if (children.size() == 1 || children.size() == 2) {
			for (S2CellId child : children) {
				if (child.isLeaf()) {
					cellIds.add(child);
				} else {
					queue.add(child);
				}
			}
		} else if (children.size() == 3) {
			cellIds.addAll(children);
		} else if (children.size() == 4) {
			//This is necessary as The Query Rect can 'just barely touch' other Bigger CellIds but still there parent would be taken into consideration.
			if (parent.level() < QUERY_APPR_LEVEL ) {
				for (S2CellId child : children) {
					queue.add(child);
				}
			} else {
				cellIds.add(parent);
			}
		} else {
			assert false; // This should not happen.
		}
	}


//	public static long generateHashKey(long geohash, int hashKeyLength) {
//		if (geohash < 0) {
//			// Counteract "-" at beginning of geohash.
//			hashKeyLength++;
//		}
//
//		String geohashString = String.valueOf(geohash);
//		long denominator = (long) Math.pow(10, geohashString.length() - hashKeyLength);
//		return geohash / denominator;
//	}
}
