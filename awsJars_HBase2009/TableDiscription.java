package awsJars;

import java.io.IOException;
import java.util.Scanner;
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

public class TableDiscription {

	public static void main(String[] args) throws IOException {

		Logger.getRootLogger().setLevel(Level.WARN);
		
		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		System.out.println("Connected");
		
		
		/******************* Input the table name *****************/
		System.out.println("\n");
		System.out.println("Select a Table...");
		TableName[] tableList = admin.listTableNames();
				
		int sel = 0;
		for (TableName val : tableList) {
			System.out.println(val.getNameAsString() + "-----> " + sel++);
		}
		Scanner sc = new Scanner(System.in);
		int input = sc.nextInt();

		if (input < 0 || input > tableList.length - 1) {
			System.out.println("Invaild Input");
			System.out.println("Exiting Program");
			System.exit(0);
		}
		sc.close();
		String table = tableList[input].getNameAsString();
		/**********************************************************/

		if (admin.tableExists(TableName.valueOf(table))) {

			HTableDescriptor tableDis = admin.getTableDescriptor(TableName.valueOf(table));

			System.out.println("The ColumnFamilies of table " + table+" are-");
			HColumnDescriptor[] colDes = tableDis.getColumnFamilies();

			for (HColumnDescriptor cf : colDes) {
				System.out.println(cf.getNameAsString());
			}

		} else {
			System.out.println("The HBase Table named " + table + " doesn't exists.");
		}

		System.out.println("Exiting Program");

	}

}
