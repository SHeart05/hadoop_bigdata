package hadoop.map_reduce.filmratinggreaterfour;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumFilmGreaterFourReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int counter = 0;
        for (IntWritable val : values) {
            sum += val.get();
            counter++;
        }
        result.set((int)(sum/counter));
        context.write(key, result);
    }
}
