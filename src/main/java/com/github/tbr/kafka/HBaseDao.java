package com.github.tbr.kafka;
public class HBaseDao {
	private HBaseTestingUtility utility;
	private static final int MAX_ROWS = 10000;
	private static final byte[] TABLE_NAME = Bytes.toBytes("test_full_log");
	private static final byte[] FAMILY = Bytes.toBytes("last");
	private static final byte[] QUALIFIER = Bytes.toBytes("t");

	public HBaseDao() {
		this.utility = new HBaseTestingUtility();
	}

	public void test() throws Exception {
		startup();
		doTest();
		shutdown();
	}

	private void doTest() {
		HTableInterface table = null;
		try {
			table = utility.createTable(TABLE_NAME, FAMILY);
			byte[] rowKey = Bytes.toBytes(123456L);
			System.out.println("putting....");
			List<Put> putList = new ArrayList<Put>(MAX_ROWS);
			for (int i = 1; i <= MAX_ROWS; i = i + 3) {
				Put put = new Put(rowKey, i);
				put.add(FAMILY, QUALIFIER, Bytes.toBytes(i + 2));
				putList.add(put);
			}
			table.put(putList);
			System.out.println("getting all ...");
			Get get = new Get(rowKey);
			Result result = table.get(get);
			for (Cell cell : result.rawCells()) {
				System.out.println(Bytes.toLong(CellUtil.cloneValue(cell)) + ",ts=" + cell.getTimestamp());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					throw new RuntimeException("Closing table instance failed", e);
				}
			}
		}
	}

	private void startup() throws Exception {
		utility.startMiniHBaseCluster(1, 2);
	}

	private void shutdown() throws Exception {
		//utility.shutdownMiniCluster();
	}

	public static void main(String[] args) throws Exception {
		HBaseDao dao = new HBaseDao();
		dao.test();
	}
}