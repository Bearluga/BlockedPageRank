package blocked;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Utils.PreProcessingNode;
import Utils.Utils;

							  //keyin, valuein, keyout, valueout
public class PreProcessBlockedMap extends Mapper<LongWritable, Text, Text, Text> {

 
  
	final static double fromNetID = 0.667;
	final static double rejectMin = 0.99 * fromNetID;
	final static double rejectLimit = rejectMin + 0.01; 
	
	 /*
	  * @params
	  * key: ignore it
	  * inputLine: srcID, srcPR, dest_list
	  * 
	  * 
	  * emit: <srcblk:src; destblk:dest>
	 */
	 @Override
	 public void map(LongWritable inkey, Text inputLine, Context context)
	   throws IOException, InterruptedException {
		 
	   String line = inputLine.toString();
	   
	   PreProcessingNode node = Utils.preProcesssLine(line);
	   
	   if (selectInputLine(node.randVal)){
	     long srcBlk = Utils.blockIDofNode(node.src);
	     long destBlk = Utils.blockIDofNode(node.dest);
	     context.write(new Text(srcBlk+":"+node.src), new Text(destBlk+":"+node.dest));
	   }
	   
	 }
	 
	 private boolean selectInputLine(double x){
		 return (((x >= rejectMin) && (x< rejectLimit) ? false : true));
	 }
}