package blocked;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Text, IntWritable, Text, IntWritable
import Utils.Utils;

public class PreProcessBlockedReduce extends Reducer<Text, Text, IntWritable, Text> {
	  
    /*
     * Input : <srcblk:src; destblk:dest>
     * Output: <key: blk,  
     *          val: text <nodeID, PR, <dest_list>:[blk:node]>
     *                        0     1    2
     */
    @Override
	  protected void reduce(Text key, java.lang.Iterable<Text> values,
	    org.apache.hadoop.mapreduce.Reducer<Text, Text, IntWritable, Text>.Context context)
	       throws IOException, InterruptedException {
	     
		 Configuration conf = context.getConfiguration();
		 
		 int N = conf.getInt("N", -2); //total number of nodes
		 if (N==-2){
			 Utils.log("error: value of N not passed to reducer");
		 }
		 
		 BlockedNodeID srcBN = new BlockedNodeID(key.toString());
		 IntWritable outputKey = new IntWritable(srcBN.blockID);
		 
		 StringBuilder sb= new StringBuilder();
		 sb.append(srcBN.nodeID+",");        // nodeID
		 
		 sb.append((double)(1.0/(double)N)); // PR
		 
		 StringBuilder listBuilder= new StringBuilder();
		 for (Text value : values) {
		   BlockedNodeID bn = new BlockedNodeID(value.toString());
			 listBuilder.append( bn.toString() + ",");
		 }
		 if(listBuilder.length() != 0){
			 listBuilder.deleteCharAt(listBuilder.length()-1);
			 sb.append(",");
		 }
		 sb.append(listBuilder);
		 String value = sb.toString(); 
		
	     context.write(outputKey, new Text(value));
	  }
	}