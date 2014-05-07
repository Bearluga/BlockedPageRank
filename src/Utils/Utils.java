package Utils;

import java.util.StringTokenizer;

public class Utils {
	public static final int N = 685230;
	public static final int NORMALIZER = 1000;
  
	
	public static double str2double(String str){
		return Double.parseDouble(str);
	}
	
	public static String double2str(double d){
		return Double.toString(d);
	}
	
	public static int str2int(String str){
		return Integer.parseInt(str);
	}
	
	public static String int2str(int i){
		return Integer.toString(i);
	}
	
	//TODO: should write to a readable log file.
	public static <T> void log(T input){
		System.out.println(input);
	}
	
	public static PreProcessingNode preProcesssLine(String line){
		StringTokenizer tokenizer = new StringTokenizer(line);
		   
		//TODO: properly check for invalid lines.
		PreProcessingNode n = new PreProcessingNode();
		
		//read SRC
		if (tokenizer.hasMoreTokens()){
			n.src = Utils.str2int(tokenizer.nextToken());
		}else{
			//SOME ERROR this.ID = -1; // error
		}
		
		//read DEST
		if (tokenizer.hasMoreTokens()){
			n.dest = Utils.str2int(tokenizer.nextToken()); 
		}else{
			//SOME ERROR this.PR=0; this.ID=-1;
		}
		
		//read rand
		if (tokenizer.hasMoreTokens()){
			n.randVal = Utils.str2double(tokenizer.nextToken()); 
		}else{
			//SOME ERROR
		}
		
		return n;
	}
	
}
