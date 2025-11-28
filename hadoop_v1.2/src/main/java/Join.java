
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Join {
    private static final String INPUT_PATH_CUSTOMERS = "input-join/customers.tbl";
    private static final String INPUT_PATH_ORDERS = "input-join/orders.tbl";
    private static final String OUTPUT_PATH = "output/join-";
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

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private final Text joinKey = new Text();
        private final Text taggedValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();


            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName(); //get le nom du fichier en input vu que ya 2 pour savoir avec qui on manopule

            //Split en column
            String[] columns = line.split("\\|"); // | est un character special donc on met \\

            if (filename.contains("customers")) {
                try {
                    String customerId = columns[0].trim();
                    String name = columns[1].trim();

                    joinKey.set(customerId);
                    taggedValue.set("CUST:" + name); // pour reduce

                    context.write(joinKey, taggedValue);
                } catch (Exception e) {
                    LOG.warning(e.getMessage());
                }

            }
            else if (filename.contains("orders")) {

                try {
                    String customerId = columns[1].trim();
                    String comment = columns[8].trim();

                    joinKey.set(customerId);
                    taggedValue.set("ORDER:" + comment); // pour reduce

                    context.write(joinKey, taggedValue);
                } catch (Exception e) {
                    LOG.warning(e.getMessage());
                }
            }

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> listCustomers = new ArrayList<>();
            List<String> listOrders = new ArrayList<>();

            for (Text val : values) {
                String valString = val.toString();

                if (valString.startsWith("CUST:")) {
                    listCustomers.add(valString.substring(5)); //enleve CUST: on commence par le 5eme element du string
                } else if (valString.startsWith("ORDER:")) {
                    listOrders.add(valString.substring(6)); //enleve ORDER: on comence par le 6eme elem du stind
                }
            }

            for (String name : listCustomers) {
                if (listOrders.isEmpty()) {
                    //rien
                } else {
                    for (String comment : listOrders) {
                        context.write(new Text(name), new Text(comment));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "GroupBy");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputValueClass(Text.class); //Text pas double

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path input = new Path(INPUT_PATH_CUSTOMERS);
        Path input2 = new Path(INPUT_PATH_ORDERS);
        FileInputFormat.setInputPaths(job, input, input2);// 2 input on utilise InputPaths et pas SetInputPath

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}