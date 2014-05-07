package blocked;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Utils.Utils;


public class BlockedPageRank {

	public static void main(String[] args) throws Exception {

	  
		Configuration conf = new Configuration();

		//delete old files
		Utils.log("Deleting Old Files");
		conf = new Configuration();
		Path path1 = new Path(args[1]);
		Path path1_1 = new Path(args[1]+".combined"); 
		Path path2 = new Path(args[2]);
		FileSystem fs = FileSystem.get(path1.toUri(), conf); 
		fs.delete(path1_1, true);
		fs.delete(path1, true);
		fs.delete(path2, true);
		Utils.log("Done deleting old files");
		//end

		//to generate average re
		Path handinOutputPath = new Path(args[3]);
	 //output average 
    FileSystem hdfs = FileSystem.get(handinOutputPath.toUri(), conf);
    FSDataOutputStream fsdos = null;
    try {
       if (fs.exists(handinOutputPath)) {
       // remove the file first
       fs.delete(handinOutputPath,true);
       }
       fsdos = fs.create(handinOutputPath);
    }catch(IOException e){
      
      e.printStackTrace();
    }
    
    
    
		/* === PREPROCESS JOB ===  */
		Utils.log("Starting Preprocessing");
		conf = new Configuration();

		conf.set("mapred.textoutputformat.separator", ","); //Prior to Hadoop 2 (YARN)

		//set a global Integer called N, number of nodes
		conf.setInt("N", Utils.N); 

		Job job1 = new Job(conf, "preprocessing");

		job1.setJarByClass(BlockedPageRank.class); // not sure

		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);

		job1.setMapperClass(PreProcessMap.class);
		job1.setReducerClass(PreProcessReduce.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		boolean b = job1.waitForCompletion(true); //blocking
		Utils.log("Done Preprocessing - "+ b );

		//Try to merge output files
		/*try{
			Utils.log("Combining pre processing files");
			Path srcPath = new Path(args[1]); 
			Path dstPath = new Path(args[1]+".combined"); 

			FileSystem hdfs = FileSystem.get(dstPath.toUri(), conf); 	
			FileUtil.copyMerge(hdfs, (srcPath), hdfs, (dstPath), false, conf, null); 
			Utils.log("Merged Files. Output: "+dstPath.toString());
		}
		catch (IOException e) { 		
			//errror
			Utils.log("IOException when merging preprocessing files");
		}*/

		/*===END Preprocessing===*/   

		long totalRE = 1;
		int passes = 0;
		String inputPath=args[1];//+".combined";
		String outputPath=args[2];
		
		while(totalRE > 0 && passes<5){
			/* === Page Rank Itr i === */   
			Utils.log("Starting PageRank Iter" + passes++);

			conf = new Configuration();
			//comma separate key value in output file.
			conf.set("mapred.textoutputformat.separator", ","); //Prior to Hadoop 2 (YARN)

			//set a global Integer called N, number of nodes
			conf.setInt("N", Utils.N); //TODO: give real N

			//make a new mapreduce job called pagerank
			Job job = new Job(conf, "pagerank");

			job.setJarByClass(PageRank.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));

			Utils.log("Waiting for completion of PageRank Iter "+passes);
			boolean b0 = job.waitForCompletion(true); //blocking
			
			// calculate termination condition
			totalRE = job.getCounters().findCounter(Counter.CT.RESIDUAL_ERROR).getValue();
			double avgError = (double)totalRE / (double) Utils.NORMALIZER / Utils.N;
			
			
			
			fsdos.writeUTF("Iteration "+(passes-1)+" avg error " +avgError+"\n");
			fsdos.flush();
			Utils.log("*Iteration "+(passes-1)+" avg error " +avgError+"\n");
			
//			Iteration 0 avg error 2.332958e+0
			Utils.log("Current total RE is "+ totalRE);
			Utils.log("Mapreduce iter "+passes+" completed - "+b0);

			//Try to merge output files
			/*try {
				Utils.log("Trying to merge files");
				Path srcPathx = new Path(outputPath); 
				Path dstPathx = new Path(outputPath+".combined"); 

				FileSystem hdfs = FileSystem.get(dstPathx.toUri(), conf); 	
				FileUtil.copyMerge(hdfs, (srcPathx), hdfs, (dstPathx), false, conf, null); 
				Utils.log("Merged Files. Output: "+dstPathx.toString());
			} 
			catch (IOException e) { 		
				//errror
				Utils.log("IOException when merging files");
			}*/

			try{
				Utils.log("Chaning input and out paths. And renaming output to input");
				//fill input with last output
				Utils.log("    current input path is:"+inputPath+" Current output path is :"+outputPath+"");				
				
				Path dstPathx = new Path(outputPath+""); 
				hdfs = FileSystem.get(dstPathx.toUri(), conf);
				inputPath=inputPath+passes;
				hdfs.rename(dstPathx, new Path(inputPath));
				outputPath += passes;
				
				Utils.log("    new input path is:"+inputPath+" New output path is :"+outputPath);				
			}
			catch(IOException e){
				Utils.log("Error while swaping input output.");
				e.printStackTrace();
			}
			/* === END Page Rank Itr i === */
		}
		fsdos.close();
		Utils.log("DONE");


	}

}
