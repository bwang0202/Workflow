package workflow;

import java.io.*;
import java.util.*;

public class Workflow{
    
    private static int SUBARRAYSIZE = 16;
    private int ascendingCounter = 1;
    private String ascending = "ascending";
    private Set<String> fileNames = new HashSet<String>();
	private String descending = "descending";
	private int k = 2;
	/**
	 * 4-byte array for de-duplication in task 3
	 */
	private byte[] prev = null;
    
    public void start(String fileName) {
    	byte[] bytes = new byte[SUBARRAYSIZE];
        byte[] helper = new byte[SUBARRAYSIZE];
        try {
        	debugPrint(fileName);
        	//task 1: sort in ascending order
	    	int totalLen = (int)(new File(fileName)).length();
	        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(fileName));
	        while (totalLen > 0) {
	       	 totalLen -= readSubArray(bufferedInputStream, bytes, helper, totalLen, 1); //1 means ascending, -1 means descending
	        }
	        BufferedInputStream[] biss = new BufferedInputStream[ascendingCounter - 1];
	        int idx = 0;
	        for (String filename : this.fileNames) {
	        	//debugPrint(filename);
	        	biss[idx++] = new BufferedInputStream(new FileInputStream(filename));
	        }
	        Sorter.kWayMerge(biss, "ascendingResult", 1); //1 means ascending, -1 means descending
	        debugPrint("ascendingResult");
	        //task 2: sort in descending order
	        ascendingCounter = 1;
	        fileNames.clear();
	        totalLen = (int)(new File(fileName)).length();
	        bufferedInputStream = new BufferedInputStream(new FileInputStream(fileName)); //reset 
	        while (totalLen > 0) {
	       	 totalLen -= readSubArray(bufferedInputStream, bytes, helper, totalLen, -1); //1 means ascending, -1 means descending
	        }
	        biss = new BufferedInputStream[ascendingCounter - 1];
	        idx = 0;
	        for (String filename : this.fileNames) {
	        	//debugPrint(filename);
	        	biss[idx++] = new BufferedInputStream(new FileInputStream(filename));
	        }
	        Sorter.kWayMerge(biss, "descendingResult", -1); //1 means ascending, -1 means descending
	        debugPrint("descendingResult");
	        //task 3:remove duplicates (by using result from task 1)
	        totalLen = (int)(new File("ascendingResult")).length();
	        bufferedInputStream = new BufferedInputStream(new FileInputStream("ascendingResult"));
	        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("deDuplicationResult"));
	        while (totalLen > 0) {
	        	totalLen -= removeDuplicates(bufferedInputStream, bufferedOutputStream, bytes, totalLen);
	        }
	        bufferedOutputStream.write(prev);
	        bufferedOutputStream.close();
	        debugPrint("deDuplicationResult");
	        //task 4: multiple by k
	        totalLen = (int)(new File(fileName)).length();
	        bufferedInputStream = new BufferedInputStream(new FileInputStream(fileName));
	        bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("multiplyByKResult"));
	        while (totalLen > 0) {
	       	 totalLen -= multiplyByK(bufferedInputStream, bufferedOutputStream, bytes, totalLen, 1);
	        }
	        bufferedOutputStream.close();
	        debugPrint("multiplyByKResult");
	        //task 5: divide by k (dividing result from task 3)
	        totalLen = (int)(new File("deDuplicationResult")).length();
	        bufferedInputStream = new BufferedInputStream(new FileInputStream("deDuplicationResult"));
	        bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("divideByKResult"));
	        while (totalLen > 0) {
	       	 totalLen -= multiplyByK(bufferedInputStream, bufferedOutputStream, bytes, totalLen, -1);
	        }
	        bufferedOutputStream.close();
	        debugPrint("divideByKResult");
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
    private int multiplyByK(BufferedInputStream bufferedInputStream,
			BufferedOutputStream bufferedOutputStream, byte[] bytes,
			int totalLen, int multiply) throws IOException, RuntimeException {
    	int len = totalLen < SUBARRAYSIZE ? totalLen : SUBARRAYSIZE;
		int nRead = bufferedInputStream.read(bytes, 0, len); //normally should be len
		if (nRead % 4 != 0) throw new RuntimeException("number of bytes read during multiplyByK is not multiple of four");
		int temp = 0;
		for (int i = 0; i < nRead; i += 4) {
			temp = 0;
			for (int j = 0; j < 4; j++) {
				temp = temp << 8;
				temp |= bytes[i+j];
			}
			if (multiply > 0) {
				temp *= k; //TODO: deal with overflow
			} else {
				temp /= k; //TODO: deal with overflow
			}
			bytes[i+3] = (byte) (temp & 0xFF);
			bytes[i+2] = (byte) ((temp & 0xFF00) >>> 8);
			bytes[i+1] = (byte) ((temp & 0xFF0000) >>> 16);
			bytes[i] = (byte) ((temp & 0xFF000000) >>> 24);
		}
		bufferedOutputStream.write(bytes);
		return nRead;
	}
	/**
     * 
     * @param bufferedInputStream
     * @param bufferedOutputStream
     * @param bytes
     * @param totalLen
     * @return
     * @throws IOException
     */
    private int removeDuplicates(BufferedInputStream bufferedInputStream,
			BufferedOutputStream bufferedOutputStream, byte[] bytes,
			int totalLen) throws IOException, RuntimeException {
		int len = totalLen < SUBARRAYSIZE ? totalLen : SUBARRAYSIZE;
		int nRead = bufferedInputStream.read(bytes, 0, len); //normally should be len
		if (this.prev == null) {
			prev = new byte[4];
			prev[0] = bytes[0];
			prev[1] = bytes[1];
			prev[2] = bytes[2];
			prev[3] = bytes[3];
		}
		if (nRead % 4 != 0) throw new RuntimeException("number of bytes read during deduplicatin is not multiple of four");
		for (int i = 0; i < nRead; i += 4) {
			if (prev[0] != bytes[i] || prev[1] != bytes[i+1] || prev[2] != bytes[i+2] || prev[3] != bytes[i+3]) {
				bufferedOutputStream.write(prev);
				prev[0] = bytes[i];
				prev[1] = bytes[i+1];
				prev[2] = bytes[i+2];
				prev[3] = bytes[i+3];
			}
		}
		return nRead;
	}

	/**
     * read in subarray and sort it and store intermediate result in files
     * @param f
     * @param bytes
     * @param helper
     * @param remainingBytes
     * @return
     * @throws IOException
     */
    private int readSubArray(BufferedInputStream f, byte[] bytes, byte[] helper, int remainingBytes, int isAscending) throws IOException{
        int len = remainingBytes > SUBARRAYSIZE ? SUBARRAYSIZE : remainingBytes;
        int nRead=f.read(bytes, 0, len);
        Sorter.mergeSort(bytes, helper, 0, len - 1, isAscending);
        String newfileName;
        if (isAscending > 0) {
        	newfileName = ascending + (ascendingCounter++);
        } else {
        	newfileName = descending + (ascendingCounter++);
        }
        FileOutputStream fileOutputStream = new FileOutputStream(newfileName);
        fileOutputStream.write(bytes, 0, len);
        fileOutputStream.close();
        this.fileNames.add(newfileName);
        return nRead;
    }

     public static void main(String[] args){
         try {
             byte[] outs = new byte[SUBARRAYSIZE * 5];
             outs[0] = 1;
             outs[1] = 2;
             outs[8] = 8;
             outs[9] = 9;
             outs[16] = -1;
             outs[17] = 3;
             File f = new File("newfile");
             FileOutputStream fileOuputStream = new FileOutputStream("newfile");
             fileOuputStream.write(outs);
             fileOuputStream.close();
             //Done writing testcase
             Workflow workflow = new Workflow();
             workflow.start("newfile");
             
         } catch (Exception e) {
        	 e.printStackTrace();
         }
 
         System.out.println("Hello World");
     }
     /**
      * print a byte file as an array of integers
      * @param fileName
     * @throws FileNotFoundException 
      */
     private void debugPrint(String fileName) throws Exception {
    	 BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fileName));
    	 int totalLen = (int)(new File(fileName)).length();
    	 if (totalLen % 4 != 0) throw new RuntimeException("file length is not multiple of 4");
    	 int temp = 0;
    	 System.out.println("printing " + fileName + ": ");
    	 while (totalLen > 0) {
    		 temp = 0;
    		 for (int i = 0; i < 4; i++) {
    			 temp = temp << 8;
    			 temp |= bis.read();
    		 }
    		 System.out.print(Integer.toHexString(temp) + ", ");
    		 totalLen -= 4;
    	 }
    	 System.out.print("\n");
    	 bis.close();
     }
}
