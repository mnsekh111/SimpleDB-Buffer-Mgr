package simpledb.buffer;

import java.util.Date;

public class BufferStats {

	private int numReads = 0;
	private int numWrites = 0;
	private Date lastWrite;
	private Date lastRead;

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

	@Override
	public String toString() {
		return "Number of reads   : " + numReads + "\n" + "Number of writes : " + numWrites + "\n"
				+ "Last read time   : " + lastRead.toString() + "\n" + "Last write time  : " + lastWrite.toString()
				+ "\n";
	}

}
