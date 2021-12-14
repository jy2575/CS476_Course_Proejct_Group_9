
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class bi_changeMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line != null) {
            String[] part = line.trim().split("\t");

            String counts = part[1];
            String[] date = part[0].split("-");
            String year = date[0];
            String month = date[1];
            String day = date[2];
            // if ((Integer.parseInt(year) == 2019 && Integer.parseInt(month) <= 12 &&
            // Integer.parseInt(month) >= 3)) {
            // int newyear = Integer.parseInt(year) + 1;
            // String fdate = month + "/" + day + "/" + newyear;
            // context.write(new Text(fdate), new Text(counts + "*"));
            // } else if ((Integer.parseInt(year) == 2020 && Integer.parseInt(month) <= 12
            // && Integer.parseInt(month) >= 3)) {
            // String fdate = month + "/" + day + "/" + year;
            // context.write(new Text(fdate), new Text(counts));
            // }

            if ((Integer.parseInt(year) == 2020 && Integer.parseInt(month) <= 8)
                    || (Integer.parseInt(year) == 2019 && Integer.parseInt(month) >= 3)) {
                int newyear = Integer.parseInt(year) + 1;
                String fdate = month + "/" + day + "/" + newyear;
                context.write(new Text(fdate), new Text(counts + "*"));
            }
            if ((Integer.parseInt(year) == 2021 && Integer.parseInt(month) <= 8)
                    || (Integer.parseInt(year) == 2020 && Integer.parseInt(month) >= 3)) {
                String fdate = month + "/" + day + "/" + year;
                context.write(new Text(fdate), new Text(counts));
            }

            // date = date.substring(6, 10) + "-" + date.substring(0, 2) + "-" +
            // date.substring(3, 5);

        }
    }
}