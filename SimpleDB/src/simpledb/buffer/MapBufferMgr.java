package simpledb.buffer;

import java.util.HashMap;
import java.util.Map;

import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.server.SimpleDB;

/**
 * @author smnatara
 *
 */
public class MapBufferMgr {

	public Map<Block, Buffer> bufferPoolMap;
	public int numAvailable;

	/**
	 * Creates a buffer manager having the specified number of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class
	 * {@link simpledb.server.SimpleDB}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
	 * first.
	 * 
	 * @param numbuffs
	 *            the number of buffer slots to allocate
	 */
	public MapBufferMgr(int numbuffs) {
		bufferPoolMap = new HashMap<>(numbuffs);
		numAvailable = numbuffs;
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txnum
	 *            the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()) {
			Buffer buff = entry.getValue();
			if (buff.isModifiedBy(txnum))
				buff.flush();
		}
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a reference to a disk block
	 * @return the pinned buffer
	 */
	synchronized Buffer pin(Block blk) {
		//printBufferPool("pin");
		Buffer buff = findExistingBuffer(blk);
	
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
			{
				return null;
			}
			buff.assignToBlock(blk);
		}
		if (!buff.isPinned()) {
			numAvailable--;
			bufferPoolMap.put(buff.block(), buff);
		}
		buff.pin();
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		//printBufferPool("pinNew");
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		buff.assignToNew(filename, fmtr);
		bufferPoolMap.put(buff.block(), buff);
		numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {
		//printBufferPool("unpin");
		buff.unpin();
		if (!buff.isPinned()) {
			numAvailable++;
		}
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	public Buffer findExistingBuffer(Block blk) {
		return bufferPoolMap.get(blk);
	}

	public Buffer chooseUnpinnedBuffer() {

		// Checks if there is some empty slots in the pool
		Buffer buff = bufferPoolMap.size() < SimpleDB.BUFFER_SIZE ? new Buffer() : null;

		// If the buffer pool is filled with either pinned or unpinned buffers
		if (buff == null) {
			int lowestLSN = Integer.MAX_VALUE;
			Buffer lowestBuffer = null;

			for (Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()) {

				if (!entry.getValue().isPinned()) {
					if (!entry.getValue().isModifiedBy(-1) && entry.getValue().getLogSequenceNumber() < lowestLSN) {
						lowestBuffer = entry.getValue();
						lowestLSN = lowestBuffer.getLogSequenceNumber();
					}
				}
			}

			// None of the unpinned buffers have been modified
			if (lowestBuffer == null) {
				for (Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()) {

					if (!entry.getValue().isPinned()) {
						if (entry.getValue().getLogSequenceNumber() != -1
								&& entry.getValue().getLogSequenceNumber() < lowestLSN) {
							lowestBuffer = entry.getValue();
							lowestLSN = lowestBuffer.getLogSequenceNumber();
						}
					}
				}
			}

			// None of the unpinned and unmodified pins have a positive lsn
			if (lowestBuffer == null) {
				for (Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()) {

					if (!entry.getValue().isPinned()) {
						lowestBuffer = entry.getValue();
						break;
					}
				}
			}

			// This will be null only when there is no unpinned buffers
			buff = lowestBuffer;
		}

		if (buff != null) {
			bufferPoolMap.remove(buff.block());
		}
		return buff;
	}

	/**
	 * Remove all calls for this function
	 * 
	 * @param from
	 */
	public void printBufferPool(String from) {
		System.out.println("Called from " + from);

		for (Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()) {
			try {
				System.out.print(entry.getKey().number() + " " + entry.getKey().fileName() + "---");
			} catch (NullPointerException ne) {
				System.out.print(" null ");
			}
		}
		System.out.println();
	}

	public void getStatistics() {
		for (Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()) {
			System.out.println(entry.getValue().getStats().toString() + entry.getValue().getInBuiltStats());
		}
		System.out.println();

	}
	
	
	//debug clear
	public void resetMap(){
		bufferPoolMap = new HashMap<Block, Buffer>();
	}
	
	public Map<Block, Buffer> getPool(){
		return bufferPoolMap;
	}
	
}
