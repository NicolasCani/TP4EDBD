import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GroupByProduitEtTotalExemplaire {
    private static final String INPUT_PATH = "input-groupBy/";
    private static final String OUTPUT_PATH = "output/groupByProduitDistincs/-";
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
        private final Text orderIdKey = new Text();
        private final Text compositeValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // skip ROW ID
            if (key.toString().equals("0")) {
                return;
            }

            //Split en column
            String[] columns = line.split(",");

            try{
                String orderId = columns[1].trim();
                String productId = columns[13].trim();
                String quantity = columns[18].trim();

                orderIdKey.set(orderId);

                compositeValue.set(productId + ";" + quantity);

                context.write(orderIdKey, compositeValue);
            } catch (Exception e){
                LOG.warning(e.getMessage());
            }


        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<String> distinctProducts = new HashSet<>();

            double totalQuantity = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(";");

                if (parts.length == 2) {
                    String pId = parts[0];
                    String qtyStr = parts[1];

                    distinctProducts.add(pId); //on add que pID

                    try {
                        totalQuantity += Double.parseDouble(qtyStr); //sum des quantite
                    } catch (NumberFormatException e) {
                        LOG.warning("erreur : " + qtyStr);
                    }
                }
            }

            int countDistinct = distinctProducts.size();

            String result = "NbProduits:" + countDistinct + " QteTotale:" + totalQuantity;

            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "GroupBy");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class); //Text pas double

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}