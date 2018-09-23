import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Driver {

	private static Configuration conf = null;
	private static Connection con = null;
	private static Admin admin = null;
	private static Table htable = null;
	private static String table = "actor";

	public static void createTable() throws IOException {
		System.out.println("Creating table named " + table);
		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(Driver.table));
		htable.addFamily(new HColumnDescriptor("P"));
		htable.addFamily(new HColumnDescriptor("R"));
		admin.createTable(htable);
		System.out.println("table "+table+" is Created");
	}

	public static void populatingTable() {
		
		System.out.println("Populating Table...");
		String fileName = "BollywoodActorRanking.csv";
		BufferedReader reader;

		try {
			reader = new BufferedReader(new FileReader(fileName));
			String line = reader.readLine(); // the header, to be ignored
			line = reader.readLine();
			
			while (line != null) {
				String[] vals = line.split(",");
				Driver.putData(vals);
				line = reader.readLine();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Table is Populated");
	}

	public static void putData(String[] vals) throws IOException {

		Put p = new Put(Bytes.toBytes(vals[0].substring(1, vals[0].length() - 1)));

		p.addColumn(Bytes.toBytes("P"), Bytes.toBytes("name"),
				Bytes.toBytes(vals[1].substring(1, vals[1].length() - 1)));
		p.addColumn(Bytes.toBytes("P"), Bytes.toBytes("movieCount"),
				Bytes.toBytes(vals[2].substring(1, vals[2].length() - 1)));

		p.addColumn(Bytes.toBytes("R"), Bytes.toBytes("ratingSum"),
				Bytes.toBytes(vals[3].substring(1, vals[3].length() - 1)));
		p.addColumn(Bytes.toBytes("R"), Bytes.toBytes("googleRank"),
				Bytes.toBytes(vals[6].substring(1, vals[6].length() - 1)));
		p.addColumn(Bytes.toBytes("R"), Bytes.toBytes("normRank"),
				Bytes.toBytes(vals[7].substring(1, vals[7].length() - 1)));

		htable.put(p);

	}

	public static void main(String[] args) throws IOException {
		Logger.getRootLogger().setLevel(Level.WARN);

		Driver.conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Driver.con = ConnectionFactory.createConnection(conf);
		Driver.admin = con.getAdmin();
		System.out.println("Connected");
	
		Driver.createTable();
	
		Driver.htable = con.getTable(TableName.valueOf(Driver.table));
		
		Driver.populatingTable();

		System.out.println("Exiting Program");
	}

}
