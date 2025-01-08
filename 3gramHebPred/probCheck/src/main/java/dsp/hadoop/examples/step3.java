package steps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;


import java.io.IOException;
import java.util.StringTokenizer;

public class step3 {

    public static class Triplet implements WritableComparable<Triplet> {
        public String w1 = "*";
        public String w2 = "*";
        public String w3 = "*";
        public int count = 0;

        public Triplet() {
        }

        public Triplet(String w) {
            String[] words = w.split(" ");
            w1 = words[0];
            w2 = words[1];
            w3 = words[2];
        }

        public String getw3() {
            return w3;
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(w1);
            out.writeUTF(w2);
            out.writeUTF(w3);
            out.writeInt(count);
        }

        public void readFields(DataInput in) throws IOException {
            w1 = in.readUTF();
            w2 = in.readUTF();
            w3 = in.readUTF();
            count = in.readInt();
        }

        public int compareTo(Triplet t) {
            // Rule 1: Prioritize triplets where isAstrix() is true
            if (this.isAstrix() && !t.isAstrix()) {
                return -1;
            } else if (!this.isAstrix() && t.isAstrix()) {
                return 1;
            }

            // Rule 2: Compare w1, prioritizing "*" to come first
            if (!w1.equals(t.w1)) {
                if (w1.equals("*")) return -1;
                if (t.w1.equals("*")) return 1;
                return w1.compareTo(t.w1);
            }

            // Rule 3: Compare w2, prioritizing "*" to come first
            if (!w2.equals(t.w2)) {
                if (w2.equals("*")) return -1;
                if (t.w2.equals("*")) return 1;
                return w2.compareTo(t.w2);
            }

            // Rule 4: Compare w3, prioritizing "*" to come first
            if (!w3.equals(t.w3)) {
                if (w3.equals("*")) return -1;
                if (t.w3.equals("*")) return 1;
                return -1 * w3.compareTo(t.w3);
            }

            // If all fields are equal
            return 0;
        }

        public int hashCode() {
            int result = 17; // Start with a non-zero constant
            result = 31 * result + (w1 != null ? w1.hashCode() : 0);
            result = 31 * result + (w2 != null ? w2.hashCode() : 0);
            result = 31 * result + (w3 != null ? w3.hashCode() : 0);
            return result & Integer.MAX_VALUE; // Ensure non-negative result
        }

        public Boolean isAstrix() {
            return w1.equals("*") && w2.equals("*") && w3.equals("*");
        }

        public String toString() {
            return w2 + " " + w1 + " " + w3;
        }

        public Boolean equals(Triplet t) {
            return w1.equals(t.w1) && w2.equals(t.w2) && w3.equals(t.w3);
        }

        public int getCount() {
            return count;
        }
    }


    public static class LineMapperClass extends Mapper<LongWritable, Text, Triplet, Text> {
    
        

        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] wordsAndProb = value.toString().split("\t")[0].split(" ");
            context.write(new Triplet(wordsAndProb[0] + " " + wordsAndProb[1] + " " + value.toString().split("\t")[1]), value);
        }
    }

    public static class LineReducerClass extends Reducer<Triplet,Text,Text,Text> {

        @Override
        public void reduce(Triplet key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for(Text val : values){
                context.write(new Text(""), new Text(val.toString()));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Triplet, Text> {
        @Override
        public int getPartition(Triplet key, Text value, int numPartitions) {
        // Ensure the partition is based only on w3
            if (key.isAstrix()) {
                return 0;
            } else {
                return (key.w1.hashCode() & Integer.MAX_VALUE) % numPartitions;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        if (args.length != 2) {
            System.err.println("Usage: App <bucket-name>");
            System.exit(1);
        }
        String bucketName = args[1];
        Configuration conf = new Configuration();
        conf.set("bucketName", bucketName);
        Job job = Job.getInstance(conf, "step3");
        job.setJarByClass(step1.class);
        
        job.setMapperClass(LineMapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(LineReducerClass.class);

        job.setMapOutputKeyClass(Triplet.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job,  new Path("s3://" + bucketName + "/output_step2"));
        FileOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output_step3"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
