
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class cleanCovidCaseMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final int MISSING = 9999;

    // @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line != null) {
            String[] part = line.split(",");
            if (part[0] != null && part[1] != null && part[1].contains("CASE_COUNT") != true) {
                String DATE_OF_INTEREST = part[0];
                String CASE_COUNT = part[1];
                DATE_OF_INTEREST = DATE_OF_INTEREST.substring(6, 10) + "-" + DATE_OF_INTEREST.substring(0, 2) + "-"
                        + DATE_OF_INTEREST.substring(3, 5);

                context.write(new Text(DATE_OF_INTEREST), new Text(CASE_COUNT));

            }
        }

    }
}