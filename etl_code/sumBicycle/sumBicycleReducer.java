
//this reducer is not used by main class!
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class sumBicycleReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (Text value : values) {
            sum += Integer.parseInt(value.toString());
        }
        context.write(new Text(key), new Text(Integer.toString(sum)));
    }
}
