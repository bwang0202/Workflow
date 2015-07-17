package workflow;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

public class FastSorter extends Sorter{
	
	public void kWayMerge(BufferedInputStream[] biss, String string, int ascending) throws IOException {
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(string));
		BufferedOutputStream dedupOut = null;
		BufferedOutputStream divideByKOut = null;
		byte[] prev = null;
		if (ascending > 0) {
			dedupOut = new BufferedOutputStream(new FileOutputStream("deDuplicationResult"));
			divideByKOut = new BufferedOutputStream(new FileOutputStream("divideByKResult"));
		}
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
			if (dedupOut != null) {
				if (prev == null || (oneInt[0] != prev[0] || oneInt[1] != prev[1] || oneInt[2] != prev[2] || oneInt[3] != prev[3])) {
					if (prev == null) prev = new byte[4];
					prev[0] = oneInt[0];
					prev[1] = oneInt[1];
					prev[2] = oneInt[2];
					prev[3] = oneInt[3];
					dedupOut.write(oneInt);
					parseInt(e.value/2, oneInt);
					divideByKOut.write(oneInt);
				}
			}
			SorterElement e2 = nextInt(biss[e.idx], e.idx);
			if (e2 != null) heap.offer(e2);
		}
		out.close();
		if (dedupOut != null) {
			dedupOut.close();
			divideByKOut.close();
		}
		
	}
}
