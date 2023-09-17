import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobTwo {

    public static class BuyerPurchaseMapper extends Mapper<LongWritable, Text, Text, CustomValue> {
        private CustomValue customValue = new CustomValue();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {

            String[] fields = value.toString().split(",");

            String buyerID = fields[1];
            float price = Float.parseFloat(fields[2]);
            customValue.setSum(price);
            customValue.setCount(1);

            context.write(new Text(buyerID), customValue);
        }
    }

    public static class BuyerPurchaseReducer extends Reducer<Text, CustomValue, Text, CustomValue> {

        private CustomValue result = new CustomValue();
        public void reduce(Text key, Iterable<CustomValue> values, Context context) throws
                IOException, InterruptedException {

            float sum = 0;
            int count = 0;

            for (CustomValue value : values) {

                count += value.getCount();
                sum += value.getSum();
            }
            result.setCount(count);
            result.setSum(sum);

            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Buyer Purchase Count and Sum");
        job.setJarByClass(JobTwo.class);
        job.setMapperClass(BuyerPurchaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomValue.class);
        job.setCombinerClass(BuyerPurchaseReducer.class);
        job.setReducerClass(BuyerPurchaseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CustomValue.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
