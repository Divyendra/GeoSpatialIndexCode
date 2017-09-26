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

import java.util.ArrayList;
import java.util.List;


public class GeohashRange {

	private long rangeMin;
	private long rangeMax;

	private long parentCellId;

	public GeohashRange(long range1, long range2) {
		this.rangeMin = Math.min(range1, range2);
		this.rangeMax = Math.max(range1, range2);
	}

	public GeohashRange(long range1, long range2, long parentCellId) {
		this.rangeMin = Math.min(range1, range2);
		this.rangeMax = Math.max(range1, range2);
		this.parentCellId = parentCellId;
	}


	public boolean tryMerge(GeohashRange range, long mergeableCellDiffThreshold) {
		//Merge only if parent cellIds at configured level match
		if (range.getParentCellId() == this.getParentCellId()) {
			if (range.getRangeMin() - this.rangeMax <= mergeableCellDiffThreshold
					&& range.getRangeMin() - this.rangeMax > 0) {
				this.rangeMax = range.getRangeMax();
				return true;
			}

			if (this.rangeMin - range.getRangeMax() <= mergeableCellDiffThreshold
					&& this.rangeMin - range.getRangeMax() > 0) {
				this.rangeMin = range.getRangeMin();
				return true;
			}
		}

		return false;
	}


	public long getRangeMin() { return rangeMin;}

	public String getRangeMinString() {return String.valueOf(rangeMin);	}

	public void setRangeMin(long rangeMin) {
		this.rangeMin = rangeMin;
	}

	public long getRangeMax() {
		return rangeMax;
	}

	public String getRangeMaxString() {
		return String.valueOf(rangeMax);
	}

	public void setRangeMax(long rangeMax) {
		this.rangeMax = rangeMax;
	}

	public long getParentCellId() {return parentCellId;	}

}
