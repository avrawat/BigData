package awsJars;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ListingTables {
	
	public static void main(String[] args) throws IOException {
		Logger.getRootLogger().setLevel(Level.WARN);
		
		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		System.out.println("Connected");
		
		// list of all tables
		System.out.println("List of tables...");
		TableName[] tableList = admin.listTableNames();
		
		if(tableList.length==0) {
			System.out.println("No HBase table present");
			System.out.println("Exiting Program");
			System.exit(0);
		}
		
		for (TableName val : tableList) {
			System.out.println(val.getNameAsString());
		}

	}

}