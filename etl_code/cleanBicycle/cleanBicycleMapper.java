
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class cleanBicycleMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line != null) {
            String[] part = line.split(",");
            if (part[1].matches("[0-9]+") && part[2] != null && (part[4].contains("0")||part[4].contains("4"))) {  
                String counts = part[1];
                String date = part[2];

                context.write(new Text(""), new Text(counts + "," + date));

            }
        }
    }}