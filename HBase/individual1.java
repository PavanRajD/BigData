import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Individual1 extends Configured implements Tool{

	public String Table_Name = "Movies";
    @SuppressWarnings("deprecation")
	@Override
    public int run(String[] argv) throws IOException {
        Configuration conf = HBaseConfiguration.create();        
        @SuppressWarnings("resource")
		HBaseAdmin admin=new HBaseAdmin(conf);        
        
        boolean isExists = admin.tableExists(Table_Name);
        
        if(isExists == false) {
	        //create table with column family
	        HTableDescriptor htb=new HTableDescriptor(Table_Name);
	        HColumnDescriptor UserFamily = new HColumnDescriptor("User");
	        HColumnDescriptor ProductFamily = new HColumnDescriptor("Product");
	        
	        htb.addFamily(UserFamily);
	        htb.addFamily(ProductFamily);
	        admin.createTable(htb);
        }
        
        try {
    		@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new FileReader("/home/mdm77/Downloads/movies_sample.txt"));
    	    String line;
    	    
    	    int row_count=0;
    	    
    	    //iterate over every line of the input file
    	    while((line = br.readLine()) != null) {
    	    	if(line.isEmpty()) continue;
    	    	
    			String[] lineArray =  line.split(":");
    	    	
    	    	row_count++;
    	    	
    	    	String productId = lineArray[1];
    	    	String userId = GetNextBufferString(br);
    	    	String profileName = GetNextBufferString(br);
    	    	String helpfulness = GetNextBufferString(br);
    	    	String score = GetNextBufferString(br);
    	    	String time = GetNextBufferString(br);
    	    	String summary = GetNextBufferString(br);
    	    	String text = GetNextBufferString(br);
    	    	
    	    	//initialize a put with row key as tweet_url
	            Put put = new Put(Bytes.toBytes(userId.trim() + productId.trim()));
	            
	            //add column data one after one
	            put.add(Bytes.toBytes("User"), Bytes.toBytes("userId"), Bytes.toBytes(userId));
	            put.add(Bytes.toBytes("User"), Bytes.toBytes("profileName"), Bytes.toBytes(profileName));
	            
	            put.add(Bytes.toBytes("Product"), Bytes.toBytes("productId"), Bytes.toBytes(productId));
	            put.add(Bytes.toBytes("Product"), Bytes.toBytes("helpfulness"), Bytes.toBytes(helpfulness));
	            put.add(Bytes.toBytes("Product"), Bytes.toBytes("score"), Bytes.toBytes(score));
	            put.add(Bytes.toBytes("Product"), Bytes.toBytes("time"), Bytes.toBytes(time));
	            put.add(Bytes.toBytes("Product"), Bytes.toBytes("summary"), Bytes.toBytes(summary));
	            put.add(Bytes.toBytes("Product"), Bytes.toBytes("text"), Bytes.toBytes(text));
	            
	            //add the put in the table
    	    	HTable hTable = new HTable(conf, Table_Name);
    	    	hTable.put(put);
    	    	hTable.close();    
	    	}
    	    
    	    System.out.println("Inserted " + row_count + " Rows");
    	    
	    } catch (FileNotFoundException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } catch (IOException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } 

      return 0;
   }
    
    private static String GetNextBufferString(BufferedReader br) {
    	try {
    		String line = br.readLine();
			if(line != null && !line.isEmpty()) {
				String[] lineArray =  line.split(":");
				if(lineArray.length < 2) return "";
				
		    	return lineArray[1];
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return "";
    }
    
    public static void main(String[] argv) throws Exception {
        int ret = ToolRunner.run(new Individual1(), argv);
        System.exit(ret);
    }
}