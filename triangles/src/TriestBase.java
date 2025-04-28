import java.util.HashSet;

public class TriestBase implements DataStreamAlgo {

	// TO-DO
	// - check flipCoin() functionality
	// - check updateCounters functionality (what are the additional counters?)
	
	private int globaltri; // tau 	
	private final int samsize; // M
	private Edge[] sample; // S
	private int sidx;
	private int time; // t

    /*
     * Constructor.
     * The parameter samsize denotes the size of the sample, i.e., the number of
     * edges that the algorithm can store.
     */
	public TriestBase(int samsize) {
		this.globaltri = 0;
		this.samsize = samsize;
		this.sample = new Edge[samsize];
		this.sidx = 0;
		this.time = 0;
	}
	
	public void handleEdge(Edge edge) {
		this.time++;
		if(this.sampleEdge(edge)) {
			sample[sidx] = edge;
			this.updateCounters(edge, 0);
		}
	}

	private boolean sampleEdge(Edge edge) {
		if(this.time <= this.samsize) {
			return true;
		} else if(this.flipCoin() == 1) {
			// if tails, choose sample edge to remove (insert over)
			// at this point all slots are full, so we pick one randomly
			this.sidx = (int)(Math.random() * samsize);
			this.updateCounters(this.sample[this.sidx], 1);
			return true;
		}
		return false;
	}

	// add: op = 0, remove: op = 1
	private void updateCounters(Edge edge, int op) {
		// compute neighborhood in sample (union of u, v neighbors)
		HashSet<Integer> neighbors = new HashSet<>();
		int u = edge.u;
		int v = edge.v;
		for(int i = 0; i < sample.length; i++) {
			int su = this.sample[i].u;
			int sv = this.sample[i].v;
			if(su == u || su == v) {
				neighbors.add(sv);
			} else if(sv == u || sv == v) {
				neighbors.add(su);
			}
		}
		if(op == 0) {
			this.globaltri += neighbors.size();
		} else if (op == 1) {
			this.globaltri -= neighbors.size();
		}
	}

	
	// heads = 0, tails = 1
	private int flipCoin() {
		// tail-bias M/t
		if(Math.random() <= ((double)this.samsize / this.time)) {
			return 1;
		} else {
			return 0;
		}
	}

	public int getEstimate() {
	       	return this.globaltri; 
	} 
}
