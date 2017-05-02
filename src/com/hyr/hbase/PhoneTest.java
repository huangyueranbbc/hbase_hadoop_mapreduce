package com.hyr.hbase;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.Test;

import com.hyr.hbase.Phone.pday;
import com.hyr.hbase.Phone.pdetail;

public class PhoneTest {

	/**
	 * 创建表
	 * 
	 * @throws IOException
	 */
	@Test
	public void testCreateTable() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2"); // 交给zookeeper处理

		HBaseAdmin admin = new HBaseAdmin(conf);
		String table = "t_cdr";

		if (admin.isTableAvailable(table)) {// 表是否存在
			admin.disableTable(table);
			admin.deleteTable(table);
		} else { // 不存在 创建
			HTableDescriptor t = new HTableDescriptor(table.getBytes());
			HColumnDescriptor cf1 = new HColumnDescriptor("cf1".getBytes()); // 列簇
			// cf1.setMaxVersions(10); //最大版本
			// admin.addColumn(table, cf1); // 在表中添加列簇
			t.addFamily(cf1);
			admin.createTable(t); // 创建表
		}

		admin.close();
	}

	/**
	 * 创建表
	 * 
	 * @throws IOException
	 */
	@Test
	public void testCreateTable2() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2"); // 交给zookeeper处理

		HBaseAdmin admin = new HBaseAdmin(conf);
		String table = "t_cdr2";

		if (admin.isTableAvailable(table)) {// 表是否存在
			admin.disableTable(table);
			admin.deleteTable(table);
		} else { // 不存在 创建
			HTableDescriptor t = new HTableDescriptor(table.getBytes());
			HColumnDescriptor cf1 = new HColumnDescriptor("cf1".getBytes()); // 列簇
			// cf1.setMaxVersions(10); //最大版本
			// admin.addColumn(table, cf1); // 在表中添加列簇
			t.addFamily(cf1);
			admin.createTable(t); // 创建表
		}

		admin.close();
	}

	/**
	 * 插入
	 * 
	 * @throws IOException
	 */
	@Test
	public void testInsert() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2"); // 交给zookeeper处理

		HTable table = new HTable(conf, "t_cdr");

		String rowkey = "18271683973_" + System.currentTimeMillis();
		Put put = new Put(rowkey.getBytes());
		put.add("cf1".getBytes(), "dest".getBytes(), "13805557773".getBytes());
		put.add("cf1".getBytes(), "type".getBytes(), "1".getBytes());
		put.add("cf1".getBytes(), "time".getBytes(), "2015-09-09 16:55:29".getBytes());

		table.put(put);

		table.close();
	}

	/**
	 * 查询
	 * 
	 * @throws IOException
	 */
	/**
	 * @throws IOException
	 */
	@Test
	public void testSelect() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2"); // 交给zookeeper处理

		HTable table = new HTable(conf, "t_cdr");

		String rowkey = "18271683974_1492655744924";
		Get get = new Get(rowkey.getBytes());

		Result result = table.get(get);

		Cell c1 = result.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
		System.out.println(new String(c1.getValue()));
		Cell c2 = result.getColumnLatestCell("cf1".getBytes(), "dest".getBytes());
		System.out.println(new String(c2.getValue()));
		Cell c3 = result.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
		System.out.println(new String(c3.getValue()));

		// List<Cell> cells = result.listCells(); // 获取所有单元格
		// for (Cell c : cells) {
		// if (c.getQualifierArray().equals("type".getBytes())) { // 打印type的值
		// System.out.println("呼叫类型" + new String(c.getValueArray()));
		// }
		//
		// if (c.getQualifierArray().equals("dest".getBytes())) { // 打印type的值
		// System.out.println("呼叫类型" + new String(c.getValueArray()));
		// }
		//
		// if (c.getQualifierArray().equals("time".getBytes())) { // 打印type的值
		// System.out.println("呼叫类型" + new String(c.getValueArray()));
		// }
		// }

		// rowkey按照字典排序
		Scan scan = new Scan();
		scan.setStartRow("18271683974_1492655740000".getBytes());
		scan.setStopRow("18271683974_1492655799999".getBytes());
		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs) {

		}

		table.close();
	}

	/**
	 * protobuf查询 15967177883 15993164059_9223372036834614697
	 * 
	 * @throws IOException
	 */
	@Test
	public void testSelect2() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2"); // 交给zookeeper处理

		HTable table = new HTable(conf, "t_cdr2");

		String rowkey = "15993164059_9223372036834614697";
		Get get = new Get(rowkey.getBytes());

		get.addColumn("cf1".getBytes(), "pday".getBytes());

		Result result = table.get(get);

		Cell cell = result.getColumnLatestCell("cf1".getBytes(), "pday".getBytes());
		pday pday = Phone.pday.parseFrom(CellUtil.cloneValue(cell));

		List<pdetail> pdetailList = pday.getPlistList();
		for (Phone.pdetail pdetail : pdetailList) {
			System.out.println("=======================================");
			System.out.println(pdetail.toString());
			// System.out.println(pdetail.getDest() + "_" + pdetail.getTime() +
			// "_" + pdetail.getType());
			System.out.println("=======================================");
			System.out.println();
		}

		table.close();
	}

	/**
	 * 插入 十个手机号 100条通话记录 满足查询 时间降序排序
	 */
	@Test
	public void insertDB() {
		List<Put> puts = new ArrayList<Put>(); // 装入集合一起插入记录

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2"); // 交给zookeeper处理
		HTable table = null;
		try {
			table = new HTable(conf, "t_cdr");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (int i = 0; i < 10; i++) {
			String rowkey;
			String phoneNum = getPhoneNum("183");

			// 100条通话记录
			for (int j = 0; j < 100; j++) {
				String phoneDate = getData("2016");
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				try {
					long dateLong = sdf.parse(phoneDate).getTime();
					// 降序
					rowkey = phoneNum + (Long.MAX_VALUE - dateLong);
					System.out.println(rowkey);

					Put put = new Put(rowkey.getBytes());
					put.add("cf1".getBytes(), "type".getBytes(), (new Random().nextInt(2) + "").getBytes());
					put.add("cf1".getBytes(), "time".getBytes(), phoneDate.getBytes());
					put.add("cf1".getBytes(), "dest".getBytes(), getPhoneNum("159").getBytes());

					puts.add(put);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				// 插入数据
				try {
					table.put(puts);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * @category protobuf 十个手机号 某一天 每个随机产生100条通话记录
	 */
	@Test
	public void insertDB2() {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2"); // 交给zookeeper处理
		HTable table = null;
		try {
			table = new HTable(conf, "t_cdr2");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (int i = 0; i < 10; i++) {
			String rowkey;
			String phoneNum = getPhoneNum("159");

			rowkey = phoneNum + "_" + (Long.MAX_VALUE - Long.parseLong("20161110")); // 降序

			// 一天通话记录
			Phone.pday.Builder pday = Phone.pday.newBuilder();

			// 100条通话记录
			for (int j = 0; j < 100; j++) {
				String phoneDate = getData2("20161110");

				// protobuf
				Phone.pdetail.Builder detail = Phone.pdetail.newBuilder();
				detail.setDest(getPhoneNum("177"));
				detail.setTime(phoneDate);
				detail.setType(new Random().nextInt(2) + "");

				pday.addPlist(detail);

			}

			// 存入某个手机 一天的记录
			Put put = new Put(rowkey.getBytes());
			put.add("cf1".getBytes(), "pday".getBytes(), pday.build().toByteArray());

			try {
				table.put(put);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 查询某个手机号 某个时段的通讯信息
	 */
	@Test
	public void scanDB() throws ParseException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2"); // 交给zookeeper处理
		HTable table = null;
		try {
			table = new HTable(conf, "t_cdr");
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// 查询 18302533619 二月份 通话账单
		Scan scan = new Scan();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

		String startRowkey = "18302533619" + (Long.MAX_VALUE - sdf.parse("20160201000000").getTime());
		String stopRowkey = "18302533619" + (Long.MAX_VALUE - sdf.parse("20160101000000").getTime());
		scan.setStartRow(startRowkey.getBytes());
		scan.setStopRow(stopRowkey.getBytes());

		try {
			ResultScanner resultScanner = table.getScanner(scan);

			for (Result result : resultScanner) {
				Cell c1 = result.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
				System.out.print(new String(CellUtil.cloneValue(c1)) + "_");
				Cell c2 = result.getColumnLatestCell("cf1".getBytes(), "dest".getBytes());
				System.out.print(new String(CellUtil.cloneValue(c2)) + "_");
				Cell c3 = result.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
				System.out.println(new String(CellUtil.cloneValue(c3)));
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @category 测试Filter。查询某个手机号 所有主叫Type=0的通话详情
	 */
	@Test
	public void scanDB2() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2"); // 交给zookeeper处理
		HTable table = null;
		try {
			table = new HTable(conf, "t_cdr");
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		// 18302533619

		// 前缀过滤
		PrefixFilter prefixFilter = new PrefixFilter("18302533619".getBytes());
		list.addFilter(prefixFilter);

		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("cf1".getBytes(),
				"type".getBytes(), CompareOp.EQUAL, "0".getBytes());
		list.addFilter(singleColumnValueFilter);

		Scan scan = new Scan();

		scan.setFilter(list);

		try {
			ResultScanner resultScanner = table.getScanner(scan);

			for (Result result : resultScanner) {
				String rowKey = new String(result.getColumnLatestCell("cf1".getBytes(), "type".getBytes()).getRow());
				System.out.print(rowKey + "------");
				Cell c1 = result.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
				System.out.print(new String(CellUtil.cloneValue(c1)) + "_");
				Cell c2 = result.getColumnLatestCell("cf1".getBytes(), "dest".getBytes());
				System.out.print(new String(CellUtil.cloneValue(c2)) + "_");
				Cell c3 = result.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
				System.out.println(new String(CellUtil.cloneValue(c3)));
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 随机生成手机号
	 * 
	 * @param prefix
	 * @return
	 */
	private String getPhoneNum(String prefix) {
		return prefix + String.format("%08d", new Random().nextInt(99999999));
	}

	/**
	 * 随机生成时间
	 * 
	 * @param year
	 * @return
	 */
	private String getData(String year) {
		Random r = new Random();
		return year + String.format("%02d%02d%02d%02d%02d",
				new Object[] { r.nextInt(12) + 1, r.nextInt(28) + 1, r.nextInt(60), r.nextInt(60), r.nextInt(60) });
	}

	/**
	 * @param prefix年月日
	 */
	private String getData2(String prefix) {
		Random r = new Random();
		return prefix + String.format("%02d%02d%02d", new Object[] { r.nextInt(60), r.nextInt(60), r.nextInt(60) });
	}

}
