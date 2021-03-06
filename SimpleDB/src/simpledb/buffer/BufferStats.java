package simpledb.buffer;

import java.util.Date;

/**
 * <b>BufferStats</b> class holds statistics about a particular buffer It's
 * wrapped inside {@link Buffer}
 * 
 * @author mns
 *
 */
public class BufferStats {

	public int numReads = 0;
	public int numWrites = 0;
	public Date lastWrite = null;
	public Date lastRead = null;
	public int totalPins = 0;
	public int totalUnPins = 0;

	public void updateTotalPins() {
		totalPins++;
	}

	public void updateTotalUnPins() {
		totalUnPins++;
	}

	public void updateNumReads() {
		numReads++;
	}

	public void updateNumWrites() {
		numWrites++;
	}

	public void updateLastWrite() {
		lastWrite = new Date();
	}

	public void updateLastRead() {
		lastRead = new Date();
	}

	public int getNumReads() {
		return numReads;
	}

	public int getNumWrites() {
		return numWrites;
	}

	public Date getLastWrite() {
		return lastWrite;
	}

	public Date getLastRead() {
		return lastRead;
	}

	public int getTotalPins() {
		return totalPins;
	}

	public int getTotalUnPins() {
		return totalUnPins;
	}

	@Override
	public String toString() {
		return "Number of reads   : " + numReads + "\n" + "Number of writes : " + numWrites + "\n"
				+ "Last read time   : " + (lastRead != null ? lastRead.toString():"No reads ") + "\n" + "Last write time  : " + (lastWrite != null?lastWrite.toString():"No writes ")
				+ "\n" + "Total pins : " + totalPins + "\n" + "Total unpins : " + totalUnPins + "\n";
	}

}
