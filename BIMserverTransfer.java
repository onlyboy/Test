package cn.edu.fudan.mmdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class BIMserverTransfer {
	private static Configuration conf = null;
	private static HConnection conn = null;
	private static HBaseAdmin admin = null;
	private static final String TABLE_NAME = "ifc";
	
	private static final String TABLE_IFC = "ifc";
	private static final String TABLE_GLOBAL = "global";
	
	
	/**
	 * 
	 * 创建表
	 * 
	 */
	@SuppressWarnings("deprecation")
	public void createTable(String tableName, String column) throws IOException {
		if (admin.tableExists(tableName)) {
			System.out.println("表" + tableName + "已经存在！");
		} else {
			HTableDescriptor desc = new HTableDescriptor(tableName);
			HColumnDescriptor hcd = new HColumnDescriptor(column);
			hcd.setMaxVersions(1);// 设置数据保存的最大版本数
			desc.addFamily(hcd);
			admin.createTable(desc);
			System.out.println("表" + tableName + "创建成功！");
		}
	}

	/**
	 * 
	 * 删除表
	 * 
	 */
	public void deleteTable(String tableName) throws IOException {
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println("表" + tableName + "删除成功！");
			} else {
				System.out.println("表" + tableName + "不存在！");
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 
	 * 将表名做MD5处理后和原来的键合并成为新的键
	 * 
	 */
	public byte[] mkRowKey(String tableName, byte[] originKey) {
		MessageDigest d;
		byte[] rowkey = null;
		try {
			d = MessageDigest.getInstance("MD5");
			byte[] ahash = d.digest(Bytes.toBytes(tableName));
			int length=originKey.length;
			rowkey = new byte[16+length];
			int offset = 0;
			offset = Bytes.putBytes(rowkey, offset, ahash, 0, 16);
			Bytes.putBytes(rowkey, offset, originKey, 0, length);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 algorithm not available!", e);
		}
		return rowkey;
	}
	/**
	 * 
	 * 将byte数组的flag和原来的键合并成为新的键
	 * 
	 */
	public byte[] mkRowKey(byte[] flag, byte[] originKey) {
		byte[] rowkey = null;
		int length = originKey.length;
		rowkey = new byte[8 + length];
		int offset = 0;
		offset = Bytes.putBytes(rowkey, offset, flag, 0, 8);
		Bytes.putBytes(rowkey, offset, originKey, 0, length);
		return rowkey;
	}
	
	/**
	 * 
	 * 将键拆分为由做MD5处理后的表名和原来的键
	 * 
	 */
	public byte[] splitRowkey(byte[] rowkey) {
		byte[][] result = new byte[2][];
		result[0] = Arrays.copyOfRange(rowkey, 0, 16);
		result[1] = Arrays.copyOfRange(rowkey, 16, rowkey.length);
		return result[1];
	}
	/**
	 * 删除表
	 * 
	 */
	public void deleteDataBase() {
		try {
			open();
			deleteTable(TABLE_NAME);
			close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 从HBase中读到map中
	 * 
	 */
	@SuppressWarnings("deprecation")
	public Map<String,Map<byte[],byte[]>> readFromHBase() {
		Map<String,Map<byte[],byte[]>> tablesContent=null;
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
			System.out.println(df.format(new Date()));// new Date()为获取当前系统时间
			
			tablesContent=new HashMap<String,Map<byte[],byte[]>>();
			open();
			createTable(TABLE_NAME, "f");
			HTableInterface table = conn.getTable(TABLE_NAME);
			
		    Scan s = new Scan();
		    s.addColumn(Bytes.toBytes("f"), Bytes.toBytes("n"));
		    s.addColumn(Bytes.toBytes("f"), Bytes.toBytes("v"));
		    s.setMaxVersions(1);
		    s.setCacheBlocks(false);
			ResultScanner rs = table.getScanner(s);
			List<String> tableNameList = new ArrayList<String>();
			List<byte[][]> keyValueList=new ArrayList<byte[][]>();
			for (Result r : rs) {
				KeyValue[] kv = r.raw();
				tableNameList.add(new String(kv[0].getValue()));
				byte[][] keyValue = new byte[2][];
				keyValue[0] = splitRowkey(kv[1].getRow());
				keyValue[1] = kv[1].getValue();
				keyValueList.add(keyValue);
			}
			String preTableName=null;
			String nowTableName=null;
			int i=0;
			int size=tableNameList.size();
			int j=0;
			for(i=0;i<size;i++){
				HashMap<byte[],byte[]> tableContent=new HashMap<byte[],byte[]>();
				preTableName=tableNameList.get(i);
				byte[][] keyValue=keyValueList.get(i);
				tableContent.put(keyValue[0], keyValue[1]);
				j++;
				for(;i<size-1;i++){
					nowTableName=tableNameList.get(i+1);
					if(preTableName.equals(nowTableName))
					{
						byte[][] keyValue1=keyValueList.get(i+1);
						tableContent.put(keyValue1[0], keyValue1[1]);
						j++;
					}
					else
						break;
				}
				tablesContent.put(preTableName, tableContent);
			}
			
			System.out.println("read from HBase "+TABLE_NAME+" "+tableNameList.size());
			System.out.println(tablesContent.size());
			System.out.println(df.format(new Date()));// new Date()为获取当前系统时间
			rs.close();
			table.close();
			close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tablesContent;
	}
	
	@SuppressWarnings("deprecation")
	public Map<String, Map<byte[], byte[]>> readFromHBase(long flag) {
		Map<String, Map<byte[], byte[]>> tablesContent = null;
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
			System.out.println("************* read from HBase " + TABLE_IFC
					+ " start " + df.format(new Date()));// new Date()为获取当前系统时间

			tablesContent = new HashMap<String, Map<byte[], byte[]>>();
			open();
			createTable(TABLE_IFC, "f");
			HTableInterface table = conn.getTable(TABLE_IFC);

			byte[] startKey = new byte[8];
			ByteBuffer bBuffer = ByteBuffer.wrap(startKey);
			LongBuffer lBuffer = bBuffer.asLongBuffer();
			lBuffer.put(flag);
			byte[] endKey = Arrays.copyOf(startKey, startKey.length);
			endKey[7]++;
			Scan s = new Scan(startKey, endKey);
			
			s.addColumn(Bytes.toBytes("f"), Bytes.toBytes("n"));
			s.addColumn(Bytes.toBytes("f"), Bytes.toBytes("v"));
			s.setMaxVersions(1);
			s.setCacheBlocks(false);
			ResultScanner rs = table.getScanner(s);
			List<String> tableNameList = new ArrayList<String>();
			List<byte[][]> keyValueList = new ArrayList<byte[][]>();
			for (Result r : rs) {
				KeyValue[] kv = r.raw();
				tableNameList.add(new String(kv[0].getValue()));
				byte[][] keyValue = new byte[2][];
				keyValue[0] = splitRowkey(kv[1].getRow());
				keyValue[1] = kv[1].getValue();
				keyValueList.add(keyValue);
			}
			String preTableName = null;
			String nowTableName = null;
			int i = 0;
			int size = tableNameList.size();
			int j = 0;
			for (i = 0; i < size; i++) {
				HashMap<byte[], byte[]> tableContent = new HashMap<byte[], byte[]>();
				preTableName = tableNameList.get(i);
				byte[][] keyValue = keyValueList.get(i);
				tableContent.put(keyValue[0], keyValue[1]);
				j++;
				for (; i < size - 1; i++) {
					nowTableName = tableNameList.get(i + 1);
					if (preTableName.equals(nowTableName)) {
						byte[][] keyValue1 = keyValueList.get(i + 1);
						tableContent.put(keyValue1[0], keyValue1[1]);
						j++;
					} else
						break;
				}
				tablesContent.put(preTableName, tableContent);
			}

			System.out.println("************* read from HBase " + TABLE_IFC
					+ " end " + tableNameList.size());
			System.out.println(tablesContent.size());
			System.out.println(df.format(new Date()));// new Date()为获取当前系统时间
			rs.close();
			table.close();
			close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tablesContent;
	}
	
	/**
	 * 将Map写到hbase中
	 * 
	 */
	
	@SuppressWarnings("deprecation")
	public void write2HBase(Long flag,
			Map<String, Map<byte[], byte[]>> tablesContent) {
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
			System.out.println("************* write to Hbase begin :"
					+ df.format(new Date()));// new Date()为获取当前系统时间、

			open();
			createTable(TABLE_IFC, "f");
			HTableInterface table = conn.getTable(TABLE_IFC);
			table.setAutoFlush(false);
			// writeBufferSize 默认为2M ,调大可以增加批量处理的吞吐量, 丢失数据的风险也会加大
			table.setWriteBufferSize(400 * 1024 * 1024);
			List<Put> puts = new ArrayList<Put>();

			byte[] bArray = new byte[8];
			ByteBuffer bBuffer = ByteBuffer.wrap(bArray);
			LongBuffer lBuffer = bBuffer.asLongBuffer();
			lBuffer.put(flag);

			for (Map.Entry<String, Map<byte[], byte[]>> entry : tablesContent
					.entrySet()) {
				String tableName = entry.getKey();
				Map<byte[], byte[]> tableContent = entry.getValue();
				for (Map.Entry<byte[], byte[]> entry2 : tableContent.entrySet()) {
					byte[] rowkey = mkRowKey(bArray, entry2.getKey());
					Put put = new Put(rowkey);
					put.add(Bytes.toBytes("f"),
							Bytes.toBytes(String.valueOf("n")),
							Bytes.toBytes(tableName));
					put.add(Bytes.toBytes("f"),
							Bytes.toBytes(String.valueOf("v")),
							entry2.getValue());
					put.setWriteToWAL(false);
					puts.add(put);
				}
			}
			table.put(puts);
			table.flushCommits();
			admin.flush(TABLE_IFC);
			System.out.println("************* write to HBase end");
			System.out.println("一共写入到" + TABLE_IFC + "表" + puts.size() + "条记录");// new
																				// Date()为获取当前系统时间
			System.out.println(df.format(new Date()));// new Date()为获取当前系统时间
			table.close();
			close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("deprecation")
	public long readFromGlobal() {
		long flag = 0L;
		try {
			open();
			createTable(TABLE_GLOBAL, "f");
			HTableInterface table = conn.getTable(TABLE_GLOBAL);

			Get g = new Get(Bytes.toBytes("global"));
			g.setMaxVersions(1);
			g.addColumn(Bytes.toBytes("f"),Bytes.toBytes("v"));
			Result rs = table.get(g);
			for (KeyValue kv : rs.raw()) {
				ByteBuffer byteBuffer = ByteBuffer.wrap(kv.getValue());
				flag = byteBuffer.getLong();
				break;
			}
			table.close();
			close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return flag;
	}
	
	@SuppressWarnings("deprecation")
	public void write2Global(Long flag) {
		try {
			open();
			createTable(TABLE_GLOBAL, "f");
			HTableInterface table = conn.getTable(TABLE_GLOBAL);
			Put put = new Put(Bytes.toBytes("global"));
			put.add(Bytes.toBytes("f"), Bytes.toBytes(String.valueOf("v")),
					Bytes.toBytes(flag));
			table.put(put);
			table.close();
			close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("deprecation")
	public void write2HBase(Map<String,Map<byte[],byte[]>> tablesContent) {
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
			System.out.println("write to Hbase begin :"+df.format(new Date()));// new Date()为获取当前系统时间、
			
			open();
			createTable(TABLE_NAME, "f");
			HTableInterface table = conn.getTable(TABLE_NAME);
			table.setAutoFlush(false);
			//writeBufferSize 默认为2M ,调大可以增加批量处理的吞吐量, 丢失数据的风险也会加大
			table.setWriteBufferSize(400*1024*1024);
			List<Put> puts=new ArrayList<Put>();
			for(Map.Entry<String,Map<byte[],byte[]>> entry : tablesContent.entrySet()){
				String tableName = entry.getKey();
				Map<byte[],byte[]> tableContent=entry.getValue();
				for(Map.Entry<byte[],byte[]> entry2 : tableContent.entrySet()){
//					System.out.println(tableName);
					byte[] rowkey=mkRowKey(tableName, entry2.getKey());
					Put put = new Put(rowkey);
					put.add(Bytes.toBytes("f"), Bytes.toBytes(String.valueOf("n")),Bytes.toBytes(tableName));
					put.add(Bytes.toBytes("f"), Bytes.toBytes(String.valueOf("v")),entry2.getValue());
					put.setWriteToWAL(false);
					puts.add(put);
				}
			}
			table.put(puts);
			table.flushCommits();
			admin.flush(TABLE_NAME);
			System.out.println("write to HBase "+TABLE_NAME+" "+puts.size());
			System.out.println("end: "+df.format(new Date()));// new Date()为获取当前系统时间
			table.close();
			close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 打开HBase
	 * 
	 */
	public void open() {
		try {
			conf = HBaseConfiguration.create();
			conn = HConnectionManager.createConnection(conf);
			admin = new HBaseAdmin(conn);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 关闭HBase
	 * 
	 */
	public void close() {
		try {
			conf.clear();
			conn.close();
			admin.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		BIMserverTransfer h = new BIMserverTransfer();
		// Map<String,Map<byte[],byte[]>> tablesContent=h.readFromHBase();
		// h.write2HBase(tablesContent);
	//	h.deleteDataBase();
		long flag=4L;
		h.write2Global(flag);
		System.out.println(h.readFromGlobal());
	}
}
