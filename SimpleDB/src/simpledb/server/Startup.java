package simpledb.server;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.buffer.MapBufferMgr;
import simpledb.file.Block;
import simpledb.remote.*;
import java.rmi.registry.*;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

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
		// mbm.printBufferPool("Main");
		// mbm.resetMap();
		// mbm.printBufferPool("Main");
		// change_major();
		// mbm.printBufferPool("After change major ");

		/* Hard debug */

		// Buffer abort Exception
		try {
			testcase1(mbm, bm);
		} catch (BufferAbortException ex) {

		}
		// Buffer replacement
		testcase2(mbm, bm);

		// Pin existing
		testcase3(mbm, bm);

		// Lowest unpinned lsn
		testcase4(mbm, bm);
	}

	public static void testcase4(MapBufferMgr mbm, BufferMgr bm) {
		mbm.printBufferPool("Before test case 4");

		Buffer b3 = mbm.getPool().get(new Block("File 3", 0));
		b3.modifiedBy = 10;
		b3.pins = 1;

		Block blm10 = new Block("File 10", 0);
		bm.pin(blm10);

		mbm.printBufferPool("After test case 4");
	}

	public static void testcase3(MapBufferMgr mbm, BufferMgr bm) {
		mbm.printBufferPool("Before test case 3");

		Buffer b2 = mbm.getPool().get(new Block("File 3", 0));
		b2.modifiedBy = 10;
		b2.pins = 1;

		Block blm1 = new Block("File 2", 0);
		bm.pin(blm1);

		System.out.println("Pin count of 2 : " + b2.pins);
		mbm.printBufferPool("After test case 3");
	}

	public static void testcase2(MapBufferMgr mbm, BufferMgr bm) {

		mbm.printBufferPool("Before test case 2");

		Buffer b2 = mbm.getPool().get(new Block("File 2", 0));
		b2.modifiedBy = 10;
		bm.unpin(b2);

		Block blm10 = new Block("File 10", 0);
		bm.pin(blm10);

		mbm.printBufferPool("After test case 2");

	}

	public static void testcase1(MapBufferMgr mbm, BufferMgr bm) {
		mbm.resetMap();
		mbm.printBufferPool("Before test case 1 ");
		System.out.println("Available : " + mbm.numAvailable);
		Map<Block, Buffer> map = mbm.getPool();
		Block blm1 = new Block("File 1", 0);
		Buffer b1 = new Buffer();
		bm.pin(blm1);

		Block blm2 = new Block("File 2", 0);
		Buffer b2 = new Buffer();
		bm.pin(blm2);

		Block blm3 = new Block("File 3", 0);
		Buffer b3 = new Buffer();
		bm.pin(blm3);

		Block blm4 = new Block("File 4", 0);
		Buffer b4 = new Buffer();
		bm.pin(blm4);

		Block blm5 = new Block("File 5", 0);
		Buffer b5 = new Buffer();
		bm.pin(blm5);

		Block blm6 = new Block("File 6", 0);
		Buffer b6 = new Buffer();
		bm.pin(blm6);

		Block blm7 = new Block("File 7", 0);
		Buffer b7 = new Buffer();
		bm.pin(blm7);

		Block blm8 = new Block("File 8", 0);
		Buffer b8 = new Buffer();
		bm.pin(blm8);

		b1.modifiedBy = -1;
		b2.modifiedBy = -1;
		b3.modifiedBy = -1;
		b4.modifiedBy = -1;
		b5.modifiedBy = -1;
		b6.modifiedBy = -1;
		b7.modifiedBy = -1;
		b8.modifiedBy = -1;

		
		//System.out.println("This is the lsn  : "+b1.logSequenceNumber);


		System.out.println("Available : " + mbm.numAvailable);
		Block blm9 = new Block("File 9", 0);
		
		try{
		bm.pin(blm9);
		}catch(BufferAbortException be){
			System.out.println("Unable to find a space!!!");
		}

		mbm.printBufferPool("Aftet test case 1 (a) ");
		
		bm.unpin(mbm.getPool().get(new Block("File 1",0)));
		bm.pin(blm9);
		
		mbm.printBufferPool("Aftet test case 1 (b)");

	}

	public static void change_major() {
		Connection conn = null;
		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();

			String cmd = "update STUDENT set MajorId=30 " + "where SName = 'amy'";
			stmt.executeUpdate(cmd);
			System.out.println("Amy is now a drama major.");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
