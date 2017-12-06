package hadoop;

import hadoop.map_reduce.countuserratings.IntSumUserIdRatingsReducer;
import hadoop.map_reduce.countuserratings.TokenizerUserIdRatingsMapper;
import hadoop.map_reduce.filmratinggreaterfour.DoubleAvgFilmGreaterFourReducer;
import hadoop.map_reduce.filmratinggreaterfour.TokenizerFilmGreaterFourMapper;
import hadoop.map_reduce.filmtitle.SelectUserFilmTitleReducer;
import hadoop.map_reduce.filmtitle.TokenizerFilmTitleMapper;
import hadoop.map_reduce.rating.IntSumRatingReducer;
import hadoop.map_reduce.rating.TokenizerRatingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

        inputJSONs.add(new Path("/TESTING/JSON/INPUT/ONEM"));
        inputJSONs.add(new Path("/TESTING/JSON/INPUT/TENM"));
        inputJSONs.add(new Path("/TESTING/JSON/INPUT/TWENTYM"));

        Path output = null;



        try {

            Configuration conf = new Configuration();

            // /opt/hadoop-2.7.3/etc/hadoop/
            conf.addResource(new Path("/opt/hadoop-2.7.3/etc/hadoop/core-site.xml"));
            conf.addResource(new Path("/opt/hadoop-2.7.3/etc/hadoop/hdfs-site.xml"));

            Job job = Job.getInstance(conf, "ratings count");
            job.setJarByClass(MovieLens.class);

            if (args[0].equals("1")) {
                //###   Anfrage 1: Ausgabe der Anzahl aller Ratings ###
                job.setMapperClass(TokenizerRatingMapper.class);
                job.setCombinerClass(IntSumRatingReducer.class);
                job.setReducerClass(IntSumRatingReducer.class);
                job.setOutputValueClass(IntWritable.class);

                output = new Path("/TESTING/JSON/OUTPUT/TASKONE");

            } else if (args[0].equals("2")) {
                //###   Anfrage 2: Ausgabe aller Filmtitel, die der Nutzer mit der ID = 10 bewertet hat
                job.setMapperClass(TokenizerFilmTitleMapper.class);
                job.setCombinerClass(SelectUserFilmTitleReducer.class);
                job.setReducerClass(SelectUserFilmTitleReducer.class);
                job.setOutputValueClass(IntWritable.class);

                output = new Path("/TESTING/JSON/OUTPUT/TASKTWO");
            } else if (args[0].equals("3")) {
                //###   Anfrage 3: Ausgabe der Anzahl aller Ratings zu jedem Nutzer
                job.setMapperClass(TokenizerUserIdRatingsMapper.class);
                job.setCombinerClass(IntSumUserIdRatingsReducer.class);
                job.setReducerClass(IntSumUserIdRatingsReducer.class);
                job.setOutputValueClass(IntWritable.class);

                output = new Path("/TESTING/JSON/OUTPUT/TASKTHREE");
            } else if (args[0].equals("4")){
                //###   Anfrage 4: Ausgabe aller Filme mit einer durchschnittlichen Bewertung >= 4.0
                job.setMapperClass(TokenizerFilmGreaterFourMapper.class);
                job.setCombinerClass(DoubleAvgFilmGreaterFourReducer.class);
                job.setReducerClass(DoubleAvgFilmGreaterFourReducer.class);
                job.setOutputValueClass(DoubleWritable.class);

                output = new Path("/TESTING/JSON/OUTPUT/TASKFOUR");
            } else {
                throw new RuntimeException("No Task in first Parameter is choosed! e.g.  $HADOOP_HOME/bin/hadoop jar xx.jar 1 1");
            }

            job.setOutputKeyClass(Text.class);

            //Set Input files
            if (args[1].equals("1")) {
                FileInputFormat.addInputPath(job, inputJSONs.get(ONEMILLION));
            } else if (args[1].equals("10")){
                FileInputFormat.addInputPath(job, inputJSONs.get(TENMILLION));
            }else if (args[1].equals("20")){
                FileInputFormat.addInputPath(job, inputJSONs.get(TWENTYMILLION));
            }else {
                throw new RuntimeException("No Dataset is choosed at second Parameter! e.g. $ADOOP_HOME/bin/hadoop jar xx.jar 1 20");
            }
            //Set Output Paths
            FileOutputFormat.setOutputPath(job, output);

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
