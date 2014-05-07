package Utils;

import java.util.ArrayList;
import java.util.StringTokenizer;


/*
 * Input to mapper and output from reduce.
 * make a node from text line
 * make a line from node 
 */
public class Node {
	//TODO: make private add getters
	public int ID;
	public double PR;
	public ArrayList<Integer> dest_list;
	
	/*
	 * Convert line from input.txt to node object
	 * line = ID, PR, dest_list
	 * example = "1,0.1212,2,3,4"
	 */
	public Node(String line){
		this.dest_list = new ArrayList<Integer>();
		
		StringTokenizer tokenizer = new StringTokenizer(line, ",");
	   
		//TODO: properly check for invalid lines.
		
		//read ID
		if (tokenizer.hasMoreTokens()){
			this.ID = Utils.str2int(tokenizer.nextToken());
		}else{
			this.ID = -1; // error
		}
		
		//read PR
		if (tokenizer.hasMoreTokens()){
			this.PR = Utils.str2double(tokenizer.nextToken()); 
		}else{
			this.PR=0; this.ID=-1;
		}
		
		//read destinations
		while (tokenizer.hasMoreTokens()) {
			this.dest_list.add(Utils.str2int(tokenizer.nextToken()));
		}
		
		/*
		String[] parts = line.split(","); //is StringTokenizer more efficient?
		this.dest_list = new ArrayList<Integer>();
		for (int i=2; i<parts.length; i++){
			this.dest_list.add(Utils.str2int(parts[i]));
		}
		*/
	}
	
	public Node(){
		this.dest_list = new ArrayList<Integer>();
		this.ID=-1;
		this.PR=0;
	}
	
	public void setPR(double newPR){
		this.PR = newPR;
	}
	public void setID(int newID){
		this.ID = newID;
	}
	public void addDest(int destID){
		this.dest_list.add(destID);
	}
	
	/*Should return comma separated string of dest_ids*/
	public String get_dest_list_string(){
		
		if(this.dest_list.size()<1){
			return "";
		}
		
		StringBuilder sb = new StringBuilder();
		for (int destID : this.dest_list){
		   sb.append(destID + ",");
		}
		return sb.substring(0, sb.length() - 1) ;
		
	}
}
