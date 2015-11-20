package simpledb.buffer;


import java.util.HashMap;
import java.util.Map;

import simpledb.file.Block;

/**
 * @author smnatara
 *
 */
public class MapBufferMgr {

	private Map<Block, Buffer> bufferMapPool;
	private int numAvailable;

	public MapBufferMgr(int numbuffs) {
		bufferMapPool = new HashMap<>(numbuffs);
		numAvailable = numbuffs;
	}

	synchronized void flushAll(int txnum) {
		for (Map.Entry<Block, Buffer> entry : bufferMapPool.entrySet()) {
			Buffer buff = entry.getValue();
			if (buff.isModifiedBy(txnum))
				buff.flush();
		}
	}

	synchronized Buffer pin(Block blk) {
		printBufferPool("pin");
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			buff.assignToBlock(blk);
		}
		if (!buff.isPinned()) {
			numAvailable--;
			bufferMapPool.put(buff.block(), buff);
		}
		buff.pin();
		return buff;
	}

	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		printBufferPool("pinNew");
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		buff.assignToNew(filename, fmtr);
		bufferMapPool.put(buff.block(), buff);
		numAvailable--;
		buff.pin();
		return buff;
	}

	synchronized void unpin(Buffer buff) {
		printBufferPool("unpin");
		buff.unpin();
		if (!buff.isPinned()) {
			numAvailable++;
			bufferMapPool.remove(buff.block());
		}
	}

	int available() {
		return numAvailable;
	}

	private Buffer findExistingBuffer(Block blk) {
		return bufferMapPool.get(blk);
	}

	private Buffer chooseUnpinnedBuffer() {
		Buffer buff = numAvailable > 0 ? new Buffer() : null;

		return buff;
	}

	/**
	 * Remove all calls for this function
	 * @param from
	 */
	private void printBufferPool(String from) {
		System.out.println("Called from " + from);

		for (Map.Entry<Block, Buffer> entry : bufferMapPool.entrySet()) {
			try {
				System.out.print(entry.getKey().number() + " ");
			} catch (NullPointerException ne) {
				System.out.print(" null ");
			}
		}
		System.out.println();
	}
}
