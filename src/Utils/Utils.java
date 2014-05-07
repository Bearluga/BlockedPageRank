package Utils;

import java.util.StringTokenizer;

import org.aspectj.org.eclipse.jdt.internal.compiler.ast.Block;

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

  private final static long[] blocks = {
      0,
      10328,
      20373,
      30629,
      40645,
      50462,
      60841,
      70591,
      80118,
      90497,
      100501,
      110567,
      120945,
      130999,
      140574,
      150953,
      161332,
      171154,
      181514,
      191625,
      202004,
      212383,
      222762,
      232593,
      242878,
      252938,
      263149,
      273210,
      283473,
      293255,
      303043,
      313370,
      323522,
      333883,
      343663,
      353645,
      363929,
      374236,
      384554,
      394929,
      404712,
      414617,
      424747,
      434707,
      444489,
      454285,
      464398,
      474196,
      484050,
      493968,
      503752,
      514131,
      524510,
      534709,
      545088,
      555467,
      565846,
      576225,
      586604,
      596585,
      606367,
      616148,
      626448,
      636240,
      646022,
      655804,
      665666,
      675448,
      685230 //very end
      };

  public static long blockIDofNode(long nodeID){
    int low = 0;
    int high = blocks.length-1;
    
    while(low<= high){
      int m = (low+high)>>1;
      if(blocks[m] == nodeID){
        return m;
      }else if(blocks[m] < nodeID){
        if(nodeID < blocks[m+1]){
          //  blocks[m] < nodeID < blocks[m+1]
          return m;
        }else{
          low = m+1;
        }
      }else{
        if(blocks[m-1] <= nodeID){
          // blocks[m-1] <= node < blocks[m]
          return m-1;
        }else{
          high = m-1;
        }
      }
    }
    
    
    return 0;
  }



}
