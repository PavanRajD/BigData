import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;


public class individual3 {
	public static String Table_Name = "Movies";
	
	public static void main(String[] args) throws Throwable {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		
		Scan scan = new Scan();
		Scan scan1 = new Scan();

	    scan.addColumn(Bytes.toBytes("Product"), Bytes.toBytes("score"));
	    scan1.addColumn(Bytes.toBytes("Product"), Bytes.toBytes("helpfulness"));

		ResultScanner scanner = hTable.getScanner(scan);
		ResultScanner scanner1 = hTable.getScanner(scan1);
		
		int totalScoreItems = 0;
		float sum = 0;
		Result result;
		while((result=scanner.next()) != null) {
			String value = Bytes.toString(result.value());
			sum += Float.parseFloat(value);
			totalScoreItems++;
		}
		
		int totalItems = 0;
		float totalHelpfulnessSum = 0;
		while((result=scanner1.next()) != null) {
			String value = Bytes.toString(result.value());
			String[] listValues = value.split("/");
			totalHelpfulnessSum += Float.parseFloat(listValues[0]);
			totalItems++;
		}
		
		System.out.println("Average: "+ (sum / totalScoreItems));
		
		System.out.println("Average: "+ (totalHelpfulnessSum / totalItems));
	}
}
