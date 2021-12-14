
//this reducer is not used by main class!
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class bi_changeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double lastyear = 0;
        double year = 0;
        for (Text value : values) {
            String count = value.toString();
            if (count.contains("*")) {
                count = count.substring(0, count.length() - 1);
                lastyear = Double.parseDouble(count);
            } else {
                year = Double.parseDouble(count);
            }
        }

        if (lastyear != 0 && year != 0) {
            Double percent = (year - lastyear) / lastyear * 100;

            context.write(new Text(key), new Text(String.format("%.2f", percent) + "%"));
        }

    }
}
