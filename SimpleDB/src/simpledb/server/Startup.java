package simpledb.server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.buffer.MapBufferMgr;
import simpledb.file.Block;
import simpledb.remote.RemoteDriver;
import simpledb.remote.RemoteDriverImpl;

public class Startup {
	public static void main(String args[]) throws Exception {
		// configure and initialize the database
		SimpleDB.init("simpleDB");

		// create a registry specific for the server on the default port
		Registry reg = LocateRegistry.createRegistry(1099);

		// and post the server entry in it
		RemoteDriver d = new RemoteDriverImpl();
		reg.rebind("simpledb", d);

		System.out.println("database server ready");

		BufferMgr bm = SimpleDB.bufferMgr();
		bm.getStatistics();

		/* Debug code */

		MapBufferMgr mbm = bm.getMapBufferMgr();
		mbm.bufferPoolMap = new HashMap<Block, Buffer>();
		
		// mbm.printBufferPool("Main");
		// mbm.resetMap();
		// mbm.printBufferPool("Main");
		// change_major();
		// mbm.printBufferPool("After change major ");

		/* Hard debug */
		
		testcase(mbm,bm);
	}

	public static void testcase(MapBufferMgr mbm, BufferMgr bm) {
		
		Block[] blocks = new Block[20];
		// 1. Create a list of files-blocks.
		for (int i = 0; i < blocks.length; i++) {
			blocks[i] = new Block("file " + i, 0);
		}

		// 2 . Initialized in MapBufferManager automatically

		// 3 . Check the number of available buffers initially.
		// They should be equal to the number of buffers
		// created as none of them have been pinned yet.

		System.out.println("Available buffers before pinning : " + mbm.numAvailable);

		// 4.Keep pinning buffers one by one and
		// check the number of available buffers.
		for (int i = 0; i < SimpleDB.BUFFER_SIZE; i++) {
			try {
				bm.pin(blocks[i]);
				System.out.println(blocks[i].fileName() + " is pinned successfully!");
				System.out.println("Available buffers : " + mbm.numAvailable);
				mbm.printBufferPool("");

			} catch (BufferAbortException be) {

				System.out.println("Unable to find a space!!!");
				mbm.printBufferPool("");
			}

		}
		
		System.out.println("Available buffers after pinning : " + mbm.numAvailable);

		//5. When all buffers have been pinned,
		//  if pin request is made again, throw an exception
		try {
			bm.pin(blocks[8]);
			System.out.println("Available buffers : " + mbm.numAvailable);
			mbm.printBufferPool("");

		} catch (BufferAbortException be) {
			System.out.println(blocks[8].fileName() + " cannot  be pinned");
			System.out.println("Buffer is full. Cannot pin");
			mbm.printBufferPool("");
		}
		
		//6.Unpin a few buffers and 
		//see if you are still getting an exception or not.
		
		bm.unpin(mbm.bufferPoolMap.get(blocks[0])); /*removing the buffer where block 0 is present */
		System.out.println("Unpinned "+blocks[0].fileName());
		mbm.printBufferPool("");
		
		try {
			bm.pin(blocks[8]);
			System.out.println(blocks[8].fileName() + " is pinned");
			System.out.println("Available buffers : " + mbm.numAvailable);
			mbm.printBufferPool("");
		} catch (BufferAbortException be) {
			System.out.println("Buffer is full. Cannot pin");
			mbm.printBufferPool("");
		}
		
		//7. Making all the buffers unmodified and lsn =-1. Trying to pin 
		// will select a random buffer
		System.out.println("Making all the buffers unmodified and lsn =-1. Trying to pin will select a random buffer");
		for (Map.Entry<Block, Buffer> entry : mbm.bufferPoolMap.entrySet()) {
			Buffer b  = entry.getValue();
			b.modifiedBy = -1;
			b.pins = 0;
			b.logSequenceNumber = -1;
		
		}
		mbm.printBufferPool("");
		
		System.out.println("Trying to pin "+blocks[9].fileName());
		bm.pin(blocks[9]);
		System.out.println(blocks[9].fileName() + " pinned successfully");
		mbm.printBufferPool("");
		
		//8. Making  two buffers modified and set logSequence number
		Buffer b1 = mbm.bufferPoolMap.get(blocks[1]);
		b1.modifiedBy = 10;
		b1.logSequenceNumber = 10;

		Buffer b2 = mbm.bufferPoolMap.get(blocks[2]);
		b2.modifiedBy = 10;
		b2.logSequenceNumber = 15;


		System.out.println("Trying to pin "+blocks[9].fileName());
		bm.pin(blocks[10]);
		System.out.println(blocks[10].fileName() + " pinned successfully");
		mbm.printBufferPool("");
		
		System.out.println("Trying to retrieve the buffer of a non existing block file 1");
		//10. Trying to retrieve the buffer of a non existing block
		if((mbm.bufferPoolMap.get(blocks[1])==null))
			System.out.println("Buffer that had this block is replaced");
		
		
	}

}
