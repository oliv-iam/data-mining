import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SONMRSingle {

    public static void main(String[] args) throws Exception {
        if(args.length != 6) {
            System.err.println("check arguments");
            System.exit(0);
        }
        int dataset_size = Integer.parseInt(args[0]);
        int transactions_per_block = Integer.parseInt(args[1]);
        double min_freq = Double.parseDouble(args[2]);
        String input_path = args[3];
        String interm_path = args[4];
        String output_path = args[5];

        Configuration conf = new Configuration();
        conf.setInt("dataset_size", dataset_size);
        conf.setInt("transactions_per_block", transactions_per_block);
        conf.setDouble("min_freq", min_freq);

        double timestart = System.currentTimeMillis();

        // first job: transactions -> candidate itemsets

        Job job1 = Job.getInstance(conf, "SONMR Single First Round");
        job1.setJarByClass(SONMRMulti.class);

        job1.setMapperClass(MapperOne.class);
        job1.setCombinerClass(ReducerOne.class);
        job1.setReducerClass(ReducerOne.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(MultiLineInputFormat.class);
        org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.setNumLinesPerSplit(job1, transactions_per_block);

        FileInputFormat.addInputPath(job1, new Path(input_path));
        FileOutputFormat.setOutputPath(job1, new Path(interm_path));

        boolean job1done = job1.waitForCompletion(false);
        if (!job1done) {
            System.exit(1);
        }

        // second job: candidate itemsets -> frequent itemsets

        Job job2 = Job.getInstance(conf, "SONMR Single Second Round");
        job2.setJarByClass(SONMRMulti.class);

        job2.setMapperClass(MapperThree.class);
        job2.setCombinerClass(Combiner.class);
        job2.setReducerClass(ReducerTwo.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(input_path));
        FileOutputFormat.setOutputPath(job2, new Path(output_path));

        Path path = new Path(interm_path + "/part-r-00000");
        job2.addCacheFile(path.toUri());

        boolean job2done = job2.waitForCompletion(false);
        if (!job2done) {
            System.exit(1);
        }

        double timeend = System.currentTimeMillis();

        System.err.println("Job: SONMR Single");
        System.err.println("Transactions per Block: " + transactions_per_block);
        System.err.println("Runtime: " + (timeend - timestart) + " ms");

        System.exit(0);

    }

}
