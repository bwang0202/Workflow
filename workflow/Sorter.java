package workflow;

import java.io.*;
import java.util.Comparator;
import java.util.PriorityQueue;
/**
 * Sorter class providing sequential sorting algorithm
 * @author Bojun Wang
 *
 */
public class Sorter {
	/**
     * Sort bytes between [0, end] in place, treat every 4 bytes as one integer
     */
    public void mergeSort(byte[] bytes, byte[] helper, int start, int end, int ascending) throws RuntimeException{
        if ((end - start + 1) % 4 != 0) {
            throw new RuntimeException("end and start not mutiple of four");
        }
        if (end - start == 3) return;
        int left = start/4;
        int right = (end - 3)/4;
        int mid = left + (right - left)/2;
        mergeSort(bytes, helper, start, mid * 4 + 3, ascending);
        mergeSort(bytes, helper, mid * 4 + 4, end, ascending);
        merge(bytes, helper, start, mid * 4 + 4, end, ascending);
    }
    /**
     * merge step, of merge sort
     * @param bytes byte array, interpreted as integer array
     * @param helper helper array necessary for merging
     * @param start start idx for first array
     * @param mid   start idx for second array
     * @param end   end idx for second array
     * @param ascending 1 means ascending order, -1 means descending order
     */
    protected void merge(byte[] bytes, byte[] helper, int start, int mid, int end, int ascending) {
        for (int i = start; i <= end; i++) {
            helper[i] = bytes[i];
        }
        int idx = start;
        int i = start, j = mid;
        while (i < mid || j <= end) {
            if (i == mid) {
                //copy from j to bytes
                bytes[idx++] = helper[j++];
                bytes[idx++] = helper[j++];
                bytes[idx++] = helper[j++];
                bytes[idx++] = helper[j++];
            } else if (j > end) {
                bytes[idx++] = helper[i++];
                bytes[idx++] = helper[i++];
                bytes[idx++] = helper[i++];
                bytes[idx++] = helper[i++];
            } else if (ascending * compare(helper, i, j) < 0) { //bytes[i] < bytes[j]
                bytes[idx++] = helper[i++];
                bytes[idx++] = helper[i++];
                bytes[idx++] = helper[i++];
                bytes[idx++] = helper[i++];
            } else {
                bytes[idx++] = helper[j++];
                bytes[idx++] = helper[j++];
                bytes[idx++] = helper[j++];
                bytes[idx++] = helper[j++];
            }
        }
    }
    /**
     * Compare bytes[i, i+3] against bytes[j, j+3] as integers
     */
    private int compare(byte[] bytes, int i, int j) {
    	int a = bytes[i], b = bytes[j];
    	for (int k = 1; k < 4; k++) {
    		a = (a << 8) | bytes[i + k];
    		b = (b << 8) | bytes[j + k];
    	}
    	if (a < b) return -1;
    	if (a > b) return 1;
    	return 0;
    }
    /**
     * Merge a number of arrays from file input streams.
     * @param biss array of file input streams to read input integer from
     * @param string file name to store merging result
     * @throws IOException 
     */
	public void kWayMerge(BufferedInputStream[] biss, String string, int ascending) throws IOException {
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(string));
		PriorityQueue<SorterElement> heap = new PriorityQueue<SorterElement>(biss.length, new Comparator<SorterElement>(){
			@Override
			public int compare(SorterElement o1, SorterElement o2) {  //min heap
				if (o1.value < o2.value) return -1 * ascending;
				if (o1.value == o2.value) return 0;
				return 1 * ascending;
			}
		});
		for (int i = 0; i < biss.length; i++) {
			SorterElement e = nextInt(biss[i], i);
			if (e != null) heap.offer(e);
		}
		byte[] oneInt = new byte[4];  //TODO: make this byte array longer to reduce # of I/O calls, improve efficiency
		while (!heap.isEmpty()) {
			SorterElement e = heap.poll();
			parseInt(e.value, oneInt);
			out.write(oneInt);
			SorterElement e2 = nextInt(biss[e.idx], e.idx);
			if (e2 != null) heap.offer(e2);
		}
		out.close();
	}
    /**
     * parse a integer value and store its four bytes respectively
     * @param value integer value to parse
     * @param oneInt byte array to store the four bytes
     */
	protected void parseInt(int value, byte[] oneInt) {
		oneInt[3] = (byte) (value & 0xFF);
		oneInt[2] = (byte) ((value & 0xFF00) >>> 8);
		oneInt[1] = (byte) ((value & 0xFF0000) >>> 16);
		oneInt[0] = (byte) ((value & 0xFF000000) >>> 24);
	}
	/**
	 * Get next integer from input stream, where most significant bits are read in first
	 * @param bufferedInputStream input stream
	 * @param idx idx of the input stream in the whole input stream array
	 * @return an element containing both the integer value and the input stream's idx. null if the end has been reached.
	 */
	protected SorterElement nextInt(BufferedInputStream bufferedInputStream, int idx) {
		int result = 0, nextRead;
		try {
		for (int i = 0; i < 4; i++) {
			result = result << 8;
			nextRead = bufferedInputStream.read();
			if (nextRead == -1) return null;
			result |= nextRead;
		}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return (new SorterElement(result, idx));
	}

    
}
