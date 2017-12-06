package hadoop;

import hadoop.map_reduce.rating.IntSumRatingReducer;
import hadoop.map_reduce.rating.TokenizerRatingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MovieLens {

    private final static int ONEMILLION = 0;
    private final static int TENMILLION = 1;
    private final static int TWENTYMILLION = 2;

    private final static Logger LOG = Logger.getLogger(MovieLens.class.getName());

    public static void main (String args[]) {
        List<Path> inputJSONs = new ArrayList<Path>();

        inputJSONs.add(new Path("/TESTING/JSON/input/movies_1m.json"));
        inputJSONs.add(new Path("/TESTING/JSON/input/movies_10m.json"));
        inputJSONs.add(new Path("/TESTING/JSON/input/movies_20m.json"));

        List<Path> outputJSONs = new ArrayList<Path>();

        inputJSONs.add(new Path("/TESTING/JSON/output"));
        inputJSONs.add(new Path("/TESTING/JSON/output"));
        inputJSONs.add(new Path("/TESTING/JSON/output"));

        try {

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "ratings count");
            job.setJarByClass(MovieLens.class);
            job.setMapperClass(TokenizerRatingMapper.class);
            job.setCombinerClass(IntSumRatingReducer.class);
            job.setReducerClass(IntSumRatingReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //Set Input files
            FileInputFormat.addInputPath(job, inputJSONs.get(ONEMILLION));
            //Set Output Paths
            FileOutputFormat.setOutputPath(job, outputJSONs.get(ONEMILLION));

            //Execute MapReduce Algorithm
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch (IOException e){
            LOG.log(Level.SEVERE, e.getMessage());
            throw new RuntimeException(e);
        }catch (InterruptedException e){
            LOG.log(Level.SEVERE, e.getMessage());
            throw new RuntimeException(e);
        }catch (ClassNotFoundException e){
            LOG.log(Level.SEVERE, e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
