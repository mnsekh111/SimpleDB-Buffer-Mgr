package simpledb.buffer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <b>BufferStats</b> class holds statistics about a particular buffer It's
 * wrapped inside {@link Buffer}
 * 
 * @author mns
 *
 */
public class BufferStats {

	private int numReads = 0;
	private int numWrites = 0;
	private Date lastWrite = null;
	private Date lastRead = null;
	private int totalPins = 0;
	private int totalUnPins = 0;
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss.SSS");

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
				+ "Last read time   : " + (lastRead != null ? sdf.format(lastRead):"No reads ") + "\n" + "Last write time  : " + (lastWrite != null?sdf.format(lastWrite):"No writes ")
				+ "\n" + "Total pins : " + totalPins + "\n" + "Total unpins : " + totalUnPins + "\n";
	}

}
