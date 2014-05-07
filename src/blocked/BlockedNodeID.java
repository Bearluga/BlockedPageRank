package blocked;

import java.util.StringTokenizer;

import Utils.Utils;

public class BlockedNodeID {

	public int blockID;	
	public int nodeID;
	
	// input string "blkID:nodeID"
	public BlockedNodeID(String str) {
		
		StringTokenizer tokenizer = new StringTokenizer(str, ":");
		
		//read blockID
		if (tokenizer.hasMoreTokens()){
			this.blockID = Utils.str2int(tokenizer.nextToken());
		}else{
			throw new Error();
		}
		
		//read nodeID
		if (tokenizer.hasMoreTokens()){
			this.nodeID = Utils.str2int(tokenizer.nextToken()); 
		}else{
			throw new Error();
		}
	}
	
	// input string "blkID:nodeID"
		public BlockedNodeID(int blockID, int nodeID) {
			this.blockID=blockID;
			this.nodeID=nodeID;
		}
	
	public String toString(){
		return this.blockID + ":"+ this.nodeID;
	}
	
}

