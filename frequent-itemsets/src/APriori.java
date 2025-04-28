import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;

public class APriori {

    private final ArrayList<HashSet<Integer>> transactions;

    // constructor which takes a collection of transactions as an argument
    public APriori(ArrayList<HashSet<Integer>> transactions) {
        this.transactions = transactions;
    }

    // return Ck+1 given Fk
    ArrayList<int[]> generate(int k, ArrayList<int[]> Fk) {
        // create arraylist for candidates
        ArrayList<int[]> candidates = new ArrayList<>();

        // generate candidates: must have first k - 1 items in common
        for(int i = 0; i < Fk.size(); i++) {
            for(int j = i + 1; j < Fk.size(); j++) {
                // check match
                boolean matching = true;
                for(int l = 0; l < k - 1; l++) {
                    if(Fk.get(i)[l] != Fk.get(j)[l]) {
                        matching = false;
                        break;
                    }
                }
                // merge and add to Ck+1 if matching
                if(matching) {
                    int[] merge = new int[k + 1];
                    for(int l = 0; l < k; l++) {
                        merge[l] = Fk.get(i)[l];
                    }
                    merge[k] = Fk.get(j)[k - 1];
                    candidates.add(merge);
                }
            }
        }
        return candidates;
    }

    // determine Fk+1 given Ck+1
    ArrayList<int[]> prune(ArrayList<int[]> Ck, int minsup, ArrayList<String> frequents) {
        // calculate support for each itemset in candidates
        int[] supports = new int[Ck.size()];
        for(HashSet<Integer> transaction : transactions) {
            for(int i = 0; i < Ck.size(); i++) {
                boolean contained = true;
                // check whether itemset present in transaction
                for(int item : Ck.get(i)){
                    if(!transaction.contains(item)) {
                        // exit loop f any item in itemset not present in transaction
                        contained = false;
                        break;
                    }
                }
                if(contained) {
                    supports[i] += 1;
                }
            }
        }

        // add itemsets to frequents if support >= minsup
        ArrayList<int[]> Fk = new ArrayList<>();
        for(int i = 0; i < supports.length; i++) {
            if(supports[i] >= minsup) {
                frequents.add(convert(Ck.get(i)));
                Fk.add(Ck.get(i));
            }
        }

        // return calculated frequent itemsets
        return Fk;
    }

    String convert(int[] arr) {
        String str = "";
        for(int i = 0; i < arr.length; i++) {
            str += arr[i];
            if (i < arr.length - 1) {
                str += " ";
            }
        }
        return str;
    }

    // main apriori: minimum frequency -> frequent itemsets
    // note: assumes lexicographic ordering
    ArrayList<String> alg(double minfreq) {

        int minsup = (int) (transactions.size() * minfreq);
        ArrayList<String> frequents = new ArrayList<>();
        int k = 1;

        // find frequent 1-itemsets
        HashMap<Integer,Integer> onecounts = new HashMap<>();
        //iterate over transactions, tracking support of each item
        for(HashSet<Integer> transaction : transactions) {
            for(int item : transaction) {
                onecounts.merge(item, 1, (a,b) -> (a + b));
            }
        }
        // iterate over map, adding frequents to both arraylists
        ArrayList<int[]> Fk = new ArrayList<>();
        for(int item : onecounts.keySet()) {
            if(onecounts.get(item) >= minsup) {
                int[] toadd = new int[1];
                toadd[0] = item;
                frequents.add(convert(toadd));
                Fk.add(toadd);
            }
        }

        while(!Fk.isEmpty()) {
            ArrayList<int[]> Ck1 = generate(k, Fk); // Ck+1
            Fk = prune(Ck1, minsup, frequents); // Fk = Fk+1
            k++;
        }

        return frequents;

    }

}
