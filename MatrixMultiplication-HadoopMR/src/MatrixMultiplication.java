package polyu.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {

        private static Integer rowsOfA, colsOfA, rowsOfB, colsOfB;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
        // Get the matrices info for future use.
            Configuration conf = context.getConfiguration();
            rowsOfA = Integer.parseInt(conf.get("rowsOfA"));
            colsOfA = Integer.parseInt(conf.get("colsOfA"));
            rowsOfB = Integer.parseInt(conf.get("rowsOfB"));
            colsOfB = Integer.parseInt(conf.get("colsOfB"));
            
        // YOUR JOB: Fill your core map code here    

        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
           
            // First store all emitted values for future use.
            // You need to use buffer to access all the values.
            List<Text> buffer = new ArrayList<Text>();            
            for (Text value : values)
                buffer.add(new Text(value));
            
            // YOUR JOB: Fill your core reduce code here
                    
        }
    }

    public static void main(String[] args) throws Exception {
        // Check the input parameters
        if (args.length != 6) {
            System.out.println("Please input correct parameters");
            return;
        } else if (!args[1].equals(args[2])) {
            System.out.println("Invalid two matrices");
            return;
        }
        
        // Read the parameters and store them
        Configuration conf = new Configuration();
        conf.set("rowsOfA", args[0]);
        conf.set("colsOfA", args[1]);
        conf.set("rowsOfB", args[2]);
        conf.set("colsOfB", args[3]);
        
        // Set the output format, key/value are delimited by ,
        conf.set("mapred.textoutputformat.separator", ",");
        
        // Initialize job related information
        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[4]));
        FileOutputFormat.setOutputPath(job, new Path(args[5]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
