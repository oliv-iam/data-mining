import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;

// MAPPER TO-DO
// - simplify candidate-reading method
// - make arr->str accessible to both methods


// subset of transactions -> apriori -> frequent itemsets in format (F,1)
class MapperOne extends Mapper<Object, Text, Text, IntWritable> {

    private static boolean init = false;
    private final static IntWritable one = new IntWritable(1);
    private static double min_freq;
    private ArrayList<HashSet<Integer>> transactions;

    protected void setup(Context context) {
        if(!init) {
            Configuration config = context.getConfiguration();
            min_freq = config.getDouble("min_freq", 0);
        }
        transactions = new ArrayList<>();
    }

    public void map(Object key, Text value, Context context) {
        // add to transactions object
        String[] trans = value.toString().split("\n");
        for (String t : trans) {
            HashSet<Integer> itemset = new HashSet<>();
            String[] items = t.split("\\s");
            for (String item : items) {
                itemset.add(Integer.parseInt(item));
            }
            transactions.add(itemset);
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        // call apriori on transactions subset
        APriori apriori = new APriori(transactions);
        ArrayList<String> frequents = apriori.alg(min_freq);
        for(String s : frequents) {
            context.write(new Text(s), one);
        }
    }

}

// for SONMRMulti
// candidates + transactions portion -> candidates, support
class MapperFour extends Mapper<Object, Text, Text, IntWritable> {

    private static boolean init = false;
    private static ArrayList<String> candidates;
    private static IntWritable one = new IntWritable(1);

    protected void setup(Context context) throws IOException {
        if(!init) {
            candidates = new ArrayList<>();
            URI uri = context.getCacheFiles()[0];
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path filepath = new Path(uri);
            FSDataInputStream inputstream = fs.open(filepath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputstream));
            String line;

            while ((line = reader.readLine()) != null) {
                String[] spl = line.split("\\s");
                String itemset = "";
                for (int i = 0; i < spl.length - 1; i++) {
                    itemset += spl[i];
                    if( i < spl.length - 2) {
                        itemset += " ";
                    }
                }
                candidates.add(itemset);
            }
            init = true;
        }
    }

    // emit itemset if present within transaction
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] transactions = value.toString().split("\n");
        for(String t : transactions) {
            HashSet<Integer> transaction = new HashSet<>();
            for(String item : t.split("\\s")) {
                transaction.add(Integer.parseInt(item));
            }
            // check itemsets
            for(String candidate : candidates) {
                boolean contained = true;
                for(String item : candidate.split("\\s")) {
                    if(!transaction.contains(Integer.parseInt(item))) {
                        contained = false;
                        break;
                    }
                }
                if(contained) {
                    context.write(new Text(candidate), one);
                }
            }
        }
    }

}

// for SONMRSingle
// candidates + one transaction -> emit if appears in transaction
class MapperThree extends Mapper<Object, Text, Text, IntWritable> {

    private static ArrayList<String> candidates;
    final IntWritable one = new IntWritable(1);
    private static boolean init = false;

    // read candidates from cachefiles
    protected void setup(Context context) throws IOException {
        if(!init) {
            candidates = new ArrayList<>();

            URI uri = context.getCacheFiles()[0];
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path filepath = new Path(uri);
            FSDataInputStream inputstream = fs.open(filepath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputstream));
            String line;

            while ((line = reader.readLine()) != null) {
                String[] spl = line.split("\\s");
                String itemset = "";
                for (int i = 0; i < spl.length - 1; i++) {
                    itemset += spl[i];
                    if(i < spl.length - 2) {
                        itemset += " ";
                    }
                }
                candidates.add(itemset);
            }
            init = true;
        }
    }

    // emit each candidate if in transaction
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        HashSet<Integer> transaction = new HashSet<>();
        for(String item : value.toString().split("\\s")) {
            transaction.add(Integer.parseInt(item));
        }
        // check itemsets
        for(String candidate : candidates) {
            boolean contained = true;
            for(String item : candidate.split("\\s")) {
                if(!transaction.contains(Integer.parseInt(item))) {
                    contained = false;
                    break;
                }
            }
            if(contained) {
                context.write(new Text(candidate), one);
            }
        }
    }

}


