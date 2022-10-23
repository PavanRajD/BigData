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


public class RetweetsFilter {
	public static String Table_Name = "Movies";
	
	public static void main(String[] args) throws Throwable {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		
		Scan scan = new Scan();

	    scan.addColumn(Bytes.toBytes("Product"), Bytes.toBytes("score"));
	    
		ResultScanner scanner = hTable.getScanner(scan);
		
		int NoOf0s = 0;
		int NoOf3s = 0;
		int NoOf5s = 0;
		int none = 0;
		Result result;
		while((result=scanner.next()) != null) {
			String value = Bytes.toString(result.value());
			float score = Float.parseFloat(value);
			if (score == 0) {
				NoOf0s++;
			} else if (score == 3) {
				NoOf3s++;
			} else if(score == 5) {
				NoOf5s++;
			} else {
				none++;
			}
		}
		

		System.out.println("# of 0's: "+ NoOf0s);
		System.out.println("# of 3's: "+ NoOf3s);
		System.out.println("# of 5's: "+ NoOf5s);
		
		System.out.println("Total: "+ (NoOf0s + NoOf3s + NoOf5s));
	}
}
