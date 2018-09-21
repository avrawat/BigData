package awsJars;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GettingData {

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

			/*************************************************************/
			Scan scan1 = new Scan();

//			System.out.println("The Column Families are\t" + scan1.getFamilies().toString());

			scan1.addColumn("ContactDetails".getBytes(), "Mobile".getBytes());
//			scan2.addFamily("Personal".getBytes());

			ResultScanner scanner1 = htable.getScanner(scan1);

			System.out.println("Scanner1...");

			for (Result res : scanner1) {
				System.out.println("Results " + res);
			}
			/*************************************************************/

			Scan scan2 = new Scan();

			ResultScanner scanner2 = htable.getScanner(scan2);
			System.out.println("Scanner2...");
			for (Result res : scanner2) {
				System.out.println(Bytes.toString(res.getRow()) + " "
						+ Bytes.toString(res.getValue("Personal".getBytes(), "Name".getBytes())) + " "
						+ Bytes.toString(res.getValue("Personal".getBytes(), "Age".getBytes())) + " "
						+ Bytes.toString(res.getValue("Personal".getBytes(), "Gender".getBytes())));
			}
			/*************************************************************/

			Scan scan3 = new Scan(); /// this is chutiya remove it
			
			scan3.addFamily(Bytes.toBytes("Employement"));
			ResultScanner scanner3 = htable.getScanner(scan3);
			System.out.println("Scanner3...");
			for (Result result = scanner3.next(); (result != null); result = scanner3.next()) {
			    for(KeyValue keyValue : result.list()) {
			        System.out.println("Qualifier : " + keyValue.getKeyString() + " : Value : " + Bytes.toString(keyValue.getValue()));
			    }
			}
					
			/*************************************************************/


		} else {

			System.out.println("The HBase Table named " + table + " doesn't exists.");
		}

		System.out.println("Exiting Program");

	}

}
