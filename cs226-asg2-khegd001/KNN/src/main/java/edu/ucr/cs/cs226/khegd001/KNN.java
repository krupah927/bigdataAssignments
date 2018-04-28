package edu.ucr.cs.cs226.khegd001;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class KNN
{
    //mapper clas for knn
    public static class KnnMapper
            extends Mapper<Object, Text, DoubleWritable, Text>{


        private Double xVal;
        private Double yVal;
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //get x and y values
            xVal=Double.parseDouble(context.getConfiguration().get("x-val"));
            yVal=Double.parseDouble(context.getConfiguration().get("x-val"));

        }


        private DoubleWritable outKey = new DoubleWritable(); // outkey is the key which is distance
        private Text outVal = new Text(); // value is the id of that particular point

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String pointID = value.toString().split(",")[0];
            double xCord = Double.parseDouble(value.toString().split(",")[1]);
            double yCord = Double.parseDouble(value.toString().split(",")[2]);
            double distance;
            //compute distance
            distance=Math.sqrt(Math.pow((xCord-xVal),2)+Math.pow((yCord-yVal),2));

            outKey=new DoubleWritable(distance);
            outVal=new Text(String.valueOf(pointID));
            //emit key and values
            context.write(outKey,outVal);

        }
    }

    //reducer class for knn
    public static class KNNReducer
            extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
        private Text result = new Text();
        int count=0; // counter to keep track of top n
        int k;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            count = 0;
            k=Integer.parseInt(context.getConfiguration().get("k"));
        }
        public void reduce(DoubleWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //get top k
            if(count<k) {
                for (Text val : values)
                    result.set(val);

                count++;
                context.write(key, result);

            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "knn");
        job.setJarByClass(KNN.class);
        job.setMapperClass(KnnMapper.class);
        //job.setCombinerClass(KNNReducer.class); // can be used but not necessary
        job.setReducerClass(KNNReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);
        Path input = new Path(args[0]); //input path
        FileInputFormat.addInputPath(job, input);
        Path output = new Path(args[1]); //output path
        FileOutputFormat.setOutputPath(job, output);

        job.getConfiguration().set("x-val",  args[2]); //set x value taken from argument
        job.getConfiguration().set("y-val", args[3]); //set y value taken from command line argument
        job.getConfiguration().set("k", args[4]); // set k value
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}