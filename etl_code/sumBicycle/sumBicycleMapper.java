
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class sumBicycleMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line != null) {
            String[] part = line.trim().split(",");

            String counts = part[0];
            String date = part[1].split(" ")[0];

            date = date.substring(6, 10) + "-" + date.substring(0, 2) + "-" + date.substring(3, 5);

            context.write(new Text(date), new Text(counts));

        }
    }
}