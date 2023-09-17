import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobOne {

    public static class BuyerAgeMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            String[] fields = value.toString().split(",");

            String buyerID = fields[0];
            String name = fields[1];
            int age = Integer.parseInt(fields[2]);
            String gender = fields[3];
            double salary = Double.parseDouble(fields[4]);
            String data = name + ", " +age +", "+ gender + ", " + salary;

            if (age >= 20 && age <= 50) {
                context.write(new Text(buyerID), new Text(data));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Buyer Age");
        job.setJarByClass(JobOne.class);
        job.setMapperClass(BuyerAgeMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
