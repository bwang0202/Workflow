package workflow;

import java.io.*;
import java.util.*;

public class Workflow{
    
    private static int SUBARRAYSIZE = 1000000;
    private int ascendingCounter = 1;
    private String ascending = "ascending";
    private Set<String> fileNames = new HashSet<String>();
    private Set<String> dFileNames = new HashSet<String>();
	private String descending = "descending";
	private int k = 2;
	private Sorter mySorter = new Sorter();
	private FastSorter myFastSorter = new FastSorter();
	private long mergeSortTime = 0;
	/**
	 * 4-byte array for de-duplication in task 3
	 */
	private byte[] prev = null;
	/**
	 * 
	 * @param string
	 * @throws Exception
	 */
	private void parellelTask(String string) throws Exception {
		byte[] bytes = new byte[SUBARRAYSIZE];
        byte[] helper = new byte[SUBARRAYSIZE];
        long startTime;
        long stopTime;
        long totalTime = 0;
      //  debugPrint(string);
        startTime = System.currentTimeMillis();
        //step 1: sequentially mergesort every subarray, and write to "ascending" and "descending" files 
        //while the subarray is in memory
        //Also compute multiplyByK result while the subarray is in memory
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(string));
        ascendingCounter = 1;
        fileNames.clear();
        dFileNames.clear();
        int totalLen = (int) (new File(string)).length();
        int len = SUBARRAYSIZE;
        int nRead;
        byte[] tempBytes = new byte[4];
        BufferedOutputStream mbk = new BufferedOutputStream(new FileOutputStream("multiplyByK"));
        while (totalLen > 0) {
        	if (totalLen < SUBARRAYSIZE) len = totalLen;
        	nRead = bis.read(bytes, 0, len);
        	if (nRead % 4 != 0) throw new RuntimeException("number of bytes read isn't multiple of four during parellelTask");
        	totalLen -= nRead;
        	//compute multiplyByK and output the result
        	multiplyHelper(mbk, bytes, nRead, 1);
        	myFastSorter.mergeSort(bytes, helper, 0, len - 1, 1);
        	//bytes contains integers in ascending order, sequentially write bytes to both "ascending" and "descending" files
        	BufferedOutputStream ascendingBos = new BufferedOutputStream(new FileOutputStream(ascending + ascendingCounter));
        	BufferedOutputStream descendingBos = new BufferedOutputStream(new FileOutputStream(descending + ascendingCounter));
        	ascendingBos.write(bytes, 0, nRead);
        	ascendingBos.close();
        	fileNames.add(ascending + ascendingCounter);
        	for (int i = nRead - 4; i >= 0; i -= 4) {
        		tempBytes[0] = bytes[i];
        		tempBytes[1] = bytes[i+1];
        		tempBytes[2] = bytes[i+2];
        		tempBytes[3] = bytes[i+3];
        		descendingBos.write(tempBytes);
        	}
        	descendingBos.close();
        	dFileNames.add(descending+ ascendingCounter);
        //	debugPrint(ascending + ascendingCounter);
        //	debugPrint(descending+ ascendingCounter);
        	ascendingCounter++;
        }
        mbk.close();
       // debugPrint("multiplyByK");
        //step 2: use kWayMerge of FastSorter, in which de-duplication and divide by k are computed during the merge.
        BufferedInputStream[] ascBiss = new BufferedInputStream[ascendingCounter - 1];
        int idx = 0;
        for (String filename : this.fileNames) {
        	ascBiss[idx++] = new BufferedInputStream(new FileInputStream(filename));
        }
        myFastSorter.kWayMerge(ascBiss, "ascendingResult", 1);
        (new Thread() {
        	public void run() {
        		try {
	        		int idx = 0;
	                BufferedInputStream[] descBiss = new BufferedInputStream[ascendingCounter - 1];
	                for (String filename : dFileNames) {
	                	descBiss[idx++] = new BufferedInputStream(new FileInputStream(filename));
	                }
	                myFastSorter.kWayMerge(descBiss, "descendingResult", -1);
        		} catch (Exception e) {
        			e.printStackTrace();
        		}
        	}
        }).run();
        
        stopTime = System.currentTimeMillis();
        System.out.println("ParellelTask takes total " + (stopTime - startTime));
       // debugPrint("ascendingResult");
       // debugPrint("descendingResult");
       // debugPrint("divideByKResult");
       // debugPrint("deDuplicationResult");
        
	}
    /**
     * Sequentially execute five tasks and measure time cost.
     * @param fileName
     */
    public void sequentialTask(String fileName) {
    	byte[] bytes = new byte[SUBARRAYSIZE];
        byte[] helper = new byte[SUBARRAYSIZE];
        long startTime;
        long stopTime;
        long totalTime = 0;
        try {
        	//debugPrint(fileName);
        	//task 1: sort in ascending order
        	startTime = System.currentTimeMillis();
	    	int totalLen = (int)(new File(fileName)).length();
	        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(fileName));
	        while (totalLen > 0) {
	       	 totalLen -= readSortSubarray(bufferedInputStream, bytes, helper, totalLen, 1); //1 means ascending, -1 means descending
	        }
	        BufferedInputStream[] biss = new BufferedInputStream[ascendingCounter - 1];
	        int idx = 0;
	        for (String filename : this.fileNames) {
	        	//debugPrint(filename);
	        	biss[idx++] = new BufferedInputStream(new FileInputStream(filename));
	        }
	        mySorter.kWayMerge(biss, "ascendingResult", 1); //1 means ascending, -1 means descending
	        stopTime = System.currentTimeMillis();
	        totalTime += stopTime - startTime;
	        System.out.println("task 1 takes " + (stopTime - startTime) + " \n");
	        //debugPrint("ascendingResult");
	        //task 2: sort in descending order
	        startTime = System.currentTimeMillis();
	        ascendingCounter = 1;
	        fileNames.clear();
	        totalLen = (int)(new File(fileName)).length();
	        bufferedInputStream = new BufferedInputStream(new FileInputStream(fileName)); //reset 
	        while (totalLen > 0) {
	       	 totalLen -= readSortSubarray(bufferedInputStream, bytes, helper, totalLen, -1); //1 means ascending, -1 means descending
	        }
	        biss = new BufferedInputStream[ascendingCounter - 1];
	        idx = 0;
	        for (String filename : this.fileNames) {
	        	//debugPrint(filename);
	        	biss[idx++] = new BufferedInputStream(new FileInputStream(filename));
	        }
	        mySorter.kWayMerge(biss, "descendingResult", -1); //1 means ascending, -1 means descending
	        stopTime = System.currentTimeMillis();
	        totalTime += stopTime - startTime;
	        System.out.println("task 2 takes " + (stopTime - startTime) + " \n");
	       // debugPrint("descendingResult");
	        //task 3:remove duplicates (by using result from task 1)
	        startTime = System.currentTimeMillis();
	        totalLen = (int)(new File("ascendingResult")).length();
	        bufferedInputStream = new BufferedInputStream(new FileInputStream("ascendingResult"));
	        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("deDuplicationResult"));
	        while (totalLen > 0) {
	        	totalLen -= removeDuplicates(bufferedInputStream, bufferedOutputStream, bytes, totalLen);
	        }
	        bufferedOutputStream.write(prev);
	        bufferedOutputStream.close();
	        stopTime = System.currentTimeMillis();
	        totalTime += stopTime - startTime;
	        System.out.println("task 3 takes " + (stopTime - startTime) + " \n");
	        //debugPrint("deDuplicationResult");
	        //task 4: multiple by k
	        startTime = System.currentTimeMillis();
	        totalLen = (int)(new File(fileName)).length();
	        bufferedInputStream = new BufferedInputStream(new FileInputStream(fileName));
	        bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("multiplyByKResult"));
	        while (totalLen > 0) {
	       	 totalLen -= multiplyByK(bufferedInputStream, bufferedOutputStream, bytes, totalLen, 1);
	        }
	        bufferedOutputStream.close();
	        stopTime = System.currentTimeMillis();
	        totalTime += stopTime - startTime;
	        System.out.println("task 4 takes " + (stopTime - startTime) + " \n");
	        //debugPrint("multiplyByKResult");
	        //task 5: divide by k (dividing result from task 3)
	        startTime = System.currentTimeMillis();
	        totalLen = (int)(new File("deDuplicationResult")).length();
	        bufferedInputStream = new BufferedInputStream(new FileInputStream("deDuplicationResult"));
	        bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("divideByKResult"));
	        while (totalLen > 0) {
	       	 totalLen -= multiplyByK(bufferedInputStream, bufferedOutputStream, bytes, totalLen, -1);
	        }
	        bufferedOutputStream.close();
	        stopTime = System.currentTimeMillis();
	        totalTime += stopTime - startTime;
	        System.out.println("task 5 takes " + (stopTime - startTime) + " \n");
	        System.out.println("SeqTask takes total " + totalTime + ", mergeSort time took " + mergeSortTime);
	        //debugPrint("divideByKResult");
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
    /**
     * 
     * @param bufferedInputStream
     * @param bufferedOutputStream
     * @param bytes
     * @param totalLen
     * @param multiply
     * @return
     * @throws IOException
     * @throws RuntimeException
     */
    private int multiplyByK(BufferedInputStream bufferedInputStream,
			BufferedOutputStream bufferedOutputStream, byte[] bytes,
			int totalLen, int multiply) throws IOException, RuntimeException {
    	int len = totalLen < SUBARRAYSIZE ? totalLen : SUBARRAYSIZE;
		int nRead = bufferedInputStream.read(bytes, 0, len); //normally should be len
		if (nRead % 4 != 0) throw new RuntimeException("number of bytes read during multiplyByK is not multiple of four");
		multiplyHelper(bufferedOutputStream, bytes, nRead, multiply);
		return nRead;
	}
    /**
     * Multiply/divide each integer in bytes array by this.k, and write result to the given output stream.
     * @param bufferedOutputStream
     * @param bytes
     * @param nRead
     * @param multiply
     * @throws IOException 
     */
    private void multiplyHelper(BufferedOutputStream bufferedOutputStream, byte[] bytes, int nRead, int multiply) throws IOException {
    	int temp = 0;
    	byte[] tempBytes = new byte[4];
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
			tempBytes[3] = (byte) (temp & 0xFF);
			tempBytes[2] = (byte) ((temp & 0xFF00) >>> 8);
			tempBytes[1] = (byte) ((temp & 0xFF0000) >>> 16);
			tempBytes[0] = (byte) ((temp & 0xFF000000) >>> 24);
			bufferedOutputStream.write(tempBytes);
		}
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
     * @param isAscending convenience variable for reusing the code in "descending" case
     * @return
     * @throws IOException
     */
    private int readSortSubarray(BufferedInputStream f, byte[] bytes, byte[] helper, int remainingBytes, int isAscending) throws IOException{
        int len = remainingBytes > SUBARRAYSIZE ? SUBARRAYSIZE : remainingBytes;
        int nRead=f.read(bytes, 0, len);
        long startTime = System.currentTimeMillis();
        mySorter.mergeSort(bytes, helper, 0, len - 1, isAscending);
        long endTime = System.currentTimeMillis();
        mergeSortTime += endTime - startTime;
        String newfileName;
        if (isAscending > 0) {
        	newfileName = ascending + (ascendingCounter++);
        } else {
        	newfileName = descending + (ascendingCounter++);
        }
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(newfileName));
        bos.write(bytes, 0, len);
        bos.close();
        this.fileNames.add(newfileName);
        return nRead;
    }

     public static void main(String[] args){
         try {
             byte[] outs = new byte[SUBARRAYSIZE * 2];
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
             workflow.sequentialTask("newfile");
             workflow.parellelTask("newfile");
             
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
     @SuppressWarnings({ "unused", "resource" })
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
