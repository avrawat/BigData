package awsJars;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class CreatingTable {

	public static void main(String[] args) throws IOException {

		Logger.getRootLogger().setLevel(Level.WARN);

		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		System.out.println("Connected");

		String table = "Employee";
		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(table));
		htable.addFamily(new HColumnDescriptor("Personal"));
		htable.addFamily(new HColumnDescriptor("ContactDetails"));
		htable.addFamily(new HColumnDescriptor("Employement"));

		System.out.println("Creating table named " + table);
		admin.createTable(htable);
		System.out.println(table + " table is created");

		System.out.println("Exiting Program");
	}
}
