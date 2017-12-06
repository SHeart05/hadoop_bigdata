package hadoop.map_reduce.rating;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TokenizerRatingMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Logger LOG = Logger.getLogger(TokenizerRatingMapper.class.getName());
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String title = "";
        // Text value = "{_id: "23", title: "..", genres: [...], ratings:[{user_id: 234}, {user_id:23}]},[..]"
        String[] movie_lines = value.toString().split("\\n");

        try{
            // Count every JSONObject {_id: ....}
            for (String line : movie_lines){
                JSONObject obj = new JSONObject(line);
                title = obj.getString("title");
                JSONArray ratings = obj.getJSONArray("ratings");

                // Count every Ratings JSONObject - {user_id: ..., ratings: ...}
                for (int i = 0; i < ratings.length(); i++) {
                    word.set(title);
                    //Map => Pair { title: "Terminator", 1 }
                    context.write(word, one);
                }
            }

        } catch (JSONException ex){
            LOG.log(Level.SEVERE, ex.getMessage());
            throw new RuntimeException(ex);
        }

    }
}
