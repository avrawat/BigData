package awsJars;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class DeleteRow {

	public static void main(String[] args) throws IOException {

		String table = "Employee";
		Logger.getRootLogger().setLevel(Level.WARN);
		
		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		System.out.println("Connected");

		if (admin.tableExists(TableName.valueOf(table))) {
			Table htable = con.getTable(TableName.valueOf(table));

			Delete deleterow = new Delete("row1".getBytes());		
			Delete deleteValue = new Delete("row2".getBytes());    
						
			deleteValue.addColumn("ContactDetails".getBytes() , "Mobile".getBytes()); // deleting latest value of the column 
		
	//		deleteValue.addColumns(family, qualifier) // delete all the version of specified column
	//		deleteValue.deleteFamily(family); //Delete all versions of all columns of the specified family.
								
			htable.delete(deleterow); // deleting a row
			htable.delete(deleteValue);
			
			System.out.println("Deleted the values");
			
		} else {

			System.out.println("The HBase Table named " + table + " doesn't exists.");
		}

		System.out.println("Exiting Program");
	}

}
