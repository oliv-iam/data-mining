import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

// first round
class ReducerOne extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, one); // switch to NullWritable
    }
}

// second round
class ReducerTwo extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static int minsup;
    private static boolean init;

    protected void setup(Context context) {
        if(!init) {
            Configuration config = context.getConfiguration();
            double min_freq = config.getDouble("min_freq", 0);
            int dataset_size = config.getInt("dataset_size", 0);
            if (min_freq * dataset_size == 0) {
                System.err.println("error occurred in reducer");
                System.exit(1);
            }
            minsup = (int) (dataset_size * min_freq);
            init = true;
        }
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable support : values) {
            sum += support.get();
        }
        System.err.println();
        if(sum >= minsup) {
            context.write(key, new IntWritable(sum));
        }
    }

}

class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable v : values) {
            sum += v.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
