import java.util.HashSet;

public class TriestImpr implements DataStreamAlgo {

    // TO-DO
    // - count neighbors more efficiently?

    private int globaltri; // tau 	
	private Edge[] sample; // S
	private int sidx;
	private int time; // t

    /*
     * Constructor.
     * The parameter samsize denotes the size of the sample, i.e., the number of
     * edges that the algorithm can store.
     */
	public TriestImpr(int samsize) {
		this.globaltri = 0;
		this.sample = new Edge[samsize];
		this.sidx = -1;
		this.time = 0;
	}

	public void handleEdge(Edge edge) {
        this.time++;
        this.updateCounters(edge);
        if(this.sampleEdge(edge)) {
            sample[sidx] = edge;
        }
    }

    private boolean sampleEdge(Edge edge) {
        if(this.time <= this.sample.length) {
            sidx++;
            return true;
        } else if(this.flipCoin() == 1) {
            // choose edge to remove
            this.sidx = (int)(Math.random() * this.sample.length);
            return true;
        }
        return false;
    }

    // heads = 0, tails = 1
	private int flipCoin() {
		// tail-bias M/t
		double rand = Math.random();
		double thres = (double)this.sample.length / this.time;
		if(rand <= thres) {
			return 1;
		} else {
			return 0;
		}
	}

    private void updateCounters(Edge edge) {
		// compute neighborhood in sample
		HashSet<Integer> uneighbors = new HashSet<>();
		HashSet<Integer> vneighbors = new HashSet<>();
		int u = edge.u;
		int v = edge.v;
		for(int i = 0; i < sample.length; i++) {
			if(this.sample[i] == null) {break;}
			int su = this.sample[i].u;
			int sv = this.sample[i].v;
			if(su == u && sv != v) {uneighbors.add(sv);}
            else if(sv == u) {uneighbors.add(su);}
			if(su == v) {vneighbors.add(sv);}
			if(sv == v) {vneighbors.add(su);}
		}
		int g = 0;
		for(int n : uneighbors) {
			if(vneighbors.contains(n)) {
				g++;
			}
		}

        int t = this.time;
        int M = this.sample.length;
        double left = ((double)(t - 1)) / M;
        double right = ((double)(t - 2)) / (M - 1);
        double eta = left * right;
        globaltri += (int)(g * eta);
    
    }

	public int getEstimate() {
	    return this.globaltri; 
	} 

}
