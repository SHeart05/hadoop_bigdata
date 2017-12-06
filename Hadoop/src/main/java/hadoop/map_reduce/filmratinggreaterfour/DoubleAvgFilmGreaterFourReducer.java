package hadoop.map_reduce.filmratinggreaterfour;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DoubleAvgFilmGreaterFourReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        int counter = 0;

        for (DoubleWritable val : values) {
            sum += val.get();
            counter++;
        }

        if (sum/(double)counter >= 4.0) {
            result.set(sum / (double) counter);
            context.write(key, result);
        }
    }
}
