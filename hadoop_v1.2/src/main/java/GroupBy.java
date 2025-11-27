
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GroupBy {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/groupBy-";
	private static final Logger LOG = Logger.getLogger(GroupBy.class.getName());

	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

		try {
			FileHandler fh = new FileHandler("out.log");
			fh.setFormatter(new SimpleFormatter());
			LOG.addHandler(fh);
		} catch (SecurityException | IOException e) {
			System.exit(1);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final static DoubleWritable profitWritable = new DoubleWritable();
        private final Text customerIdText = new Text();

        @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // skip ROW ID
            if (key.toString().equals("0")) {
                return;
            }

            //Split en column
            String[] columns = line.split(",");

            //Get les column important
            String customerId = columns[5];
            String profitStr = columns[20];

            try {
                //Conversion du profit en Double
                double profit = Double.parseDouble(profitStr);

                customerIdText.set(customerId);
                profitWritable.set(profit);

                //Cle = CustomerID, Valeur = Profit
                context.write(customerIdText, profitWritable);

            } catch (NumberFormatException e) {
                LOG.warning("Erreur" + e.getMessage());
            }


		}
	}

	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;


            for (DoubleWritable val : values)
                sum += val.get();

            context.write(key, new DoubleWritable(sum));
        }
    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "GroupBy");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}