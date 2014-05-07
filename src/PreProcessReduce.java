import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Text, IntWritable, Text, IntWritable
import Utils.Utils;

public class PreProcessReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
	  @Override
	  protected void reduce(IntWritable key, java.lang.Iterable<Text> values,
	    org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text>.Context context)
	       throws IOException, InterruptedException {
	     
		 Configuration conf = context.getConfiguration();
		 
		 int N = conf.getInt("N", -2); //total number of nodes
		 if (N==-2){
			 Utils.log("error: value of N not passed to reducer");
		 }
		 
		 StringBuilder sb= new StringBuilder();
		 sb.append((double)(1.0/(double)N));
		 
		 StringBuilder listBuilder= new StringBuilder();
		 for (Text value : values) {
			 listBuilder.append(Utils.str2int(value.toString()) + ",");
		 }
		 if(listBuilder.length() != 0){
			 listBuilder.deleteCharAt(listBuilder.length()-1);
			 sb.append(",");
		 }
		 sb.append(listBuilder);
		 String value = sb.toString(); 
		
	     context.write(key, new Text(value));
	  }
	}