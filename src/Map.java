import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Utils.Node;
import Utils.Utils;

							  //keyin, valuein, keyout, valueout
public class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
	 
	 /*
	  * @params
	  * key: ignore it
	  * inputLine: srcID, srcPR, dest_list
	 */
	 @Override
	 public void map(LongWritable inkey, Text inputLine, Context context)
	   throws IOException, InterruptedException {
		 
	   String line = inputLine.toString();
	   
	   Node node = new Node(line);
	   
	   for (int destID : node.dest_list){
		   //emit (dist_ID,  srcPR/dest_list.length())  //a.k.a edgeVal
		   int key = destID;
		   String value = "g"+Utils.double2str(node.PR/node.dest_list.size());
		   context.write(new IntWritable(key), new Text(value)); //emit
	   }
	   
	   //emit(srcID,   dest_list)
	   String value = "o"+node.get_dest_list_string();
	   context.write(new IntWritable(node.ID), new Text(value));
	   
	   //emit(srcID, PR)
	   value = "b"+Utils.double2str(node.PR);
	   context.write(new IntWritable(node.ID), new Text(value));
	 }
}