import java.util.HashSet;

public class TriestBase implements DataStreamAlgo {

	// TO-DO
	// - check flipCoin() functionality
	// - check updateCounters functionality (what are the additional counters?)
	// - find neighbors more efficiently
	
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
		this.sidx = -1;
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
			sidx++;
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
		// compute neighborhood in sample
		HashSet<Integer> uneighbors = new HashSet<>();
		HashSet<Integer> vneighbors = new HashSet<>();
		int u = edge.u;
		int v = edge.v;
		for(int i = 0; i < sample.length; i++) {
			if(this.sample[i] == null) {break;}
			int su = this.sample[i].u;
			int sv = this.sample[i].v;
			if(su == u) {uneighbors.add(sv);}
			if(sv == u) {uneighbors.add(su);}
			if(su == v) {vneighbors.add(sv);}
			if(sv == v) {vneighbors.add(su);}
		}
		int intersection = 0;
		for(int n : uneighbors) {
			if(vneighbors.contains(n)) {
				intersection++;
			}
		}
		if(op == 0) {
			this.globaltri += intersection;
		} else if (op == 1) {
			this.globaltri -= intersection;
		}
	}

	
	// heads = 0, tails = 1
	private int flipCoin() {
		// tail-bias M/t
		double rand = Math.random();
		double thres = (double)this.samsize / this.time;
		if(rand <= thres) {
			return 1;
		} else {
			return 0;
		}
	}

	public int getEstimate() {
	    return this.globaltri; 
	} 

	public String toString() {
		String str = "";
		for(int i = 0; i < sample.length; i++) {
			if(sample[i] == null) {break;}
			str += sample[i].toString() + "    ";
		}
		return str;
	}
	
}
