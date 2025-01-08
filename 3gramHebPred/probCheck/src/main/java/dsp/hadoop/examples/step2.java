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

public class step2 {

    public static class Triplet implements WritableComparable<Triplet> {
        public String w1 = "*";
        public String w2 = "*";
        public String w3 = "*";
        public int count = 0;

        public Triplet() {
        }

        public Triplet(String w) {
            String[] words = w.split(" ");
            if (words.length == 3) { //w2 w1 w3
                w1 = words[1];
                w2 = words[0];
                w3 = words[2];
                count = 3;
            } else if (words.length == 2) { //w2 w1
                w1 = words[1];
                w2 = words[0];
                count = 2;
            } else { //w1
                w1 = words[0];
                count = 1;
            }
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
                return w3.compareTo(t.w3);
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
        Boolean first = true;
        String lastNgram = "";
        Long lastNgramCount = 0L;
        List<String> stopwords = new ArrayList<String>();

        public ArrayList<String> loadStopWords(Context context) {
            ArrayList<String> stopwords = new ArrayList<String>();
            try {
                AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion("us-east-1")
                .build();
                String bucketName = context.getConfiguration().get("bucketName");
                BufferedReader reader = new BufferedReader(new InputStreamReader(s3.getObject(bucketName , "input/stop.txt").getObjectContent()));

                String line;
                while ((line = reader.readLine()) != null) {
                    stopwords.add(line);
                    System.out.println("stopword: " + line);
                }

                // Close the reader
                reader.close();

            } catch (Exception e) {
                    System.err.println("Error occurred while reading from S3: " + e.getMessage());
                    e.printStackTrace();
            }
            return stopwords;
        }

        @Override
        public void setup(Context context) {
            stopwords = loadStopWords(context);
        }

        public Text getN3(Text ngram) {
            String[] ns = ngram.toString().split("\t")[1].split(" ");
            return new Text(ns[2].split(":")[1]);
        }

        

        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] words = value.toString().split("\t")[0].split(" ");

            context.write(new Triplet(value.toString().split("\t")[0]), new Text(value.toString().split("\t")[1])); //w1w2w3
            context.write(new Triplet(words[0] + " " + words[1]), getN3(value)); //w1w2
            context.write(new Triplet(words[0]), getN3(value)); //w1
            context.write(new Triplet(words[1] + " " + words[2]), getN3(value)); //w2w3
            context.write(new Triplet(words[1]), getN3(value)); //w2
            context.write(new Triplet(words[2]), getN3(value)); //w3
        }
    }

    public static class LineProb {
        String line;
        long N1; // w3 occurrences
        long N2; // w2w3 occurrences
        long N3; // w1w2w3 occurrences
        long C0; // total word instances in corpus
        long C1; // w2 occurrences
        long C2; // w1w2 occurrences

        public LineProb(String line, long N1, long N2, long N3, long C0, long C1, long C2) {
            this.line = line;
            this.N1 = N1;
            this.N2 = N2;
            this.N3 = N3;
            this.C0 = C0;
            this.C1 = C1;
            this.C2 = C2;
        }

        public static LineProb start(String line , long c0) {
            return new LineProb(line, 0, 0, 0, c0, 0, 0);
        }

        public static LineProb dupeN3(LineProb lp , String line ,  long n3) {
            return new LineProb(line, lp.N1, lp.N2, n3, lp.C0, lp.C1, lp.C2);
        }

        public void addN1(long n1) {
            N1 += n1;
        }

        public void addN2(long n2) {
            N2 += n2;
        }

        public void addN3(long n3) {
            N3 += n3;
        }

        public void addC0(long c0) {
            C0 += c0;
        }

        public void addC1(long c1) {
            C1 += c1;
        }

        public void addC2(long c2) {
            C2 += c2;
        }

        public long getN1() {
            return N1;
        }

        public long getN2() {
            return N2;
        }

        public long getN3() {
            return N3;
        }

        public long getC0() {
            return C0;
        }

        public long getC1() {
            return C1;
        }

        public long getC2() {
            return C2;
        }

        public void setN1(long n1) {
            N1 = n1;
        }

        public void setN2(long n2) {
            N2 = n2;
        }

        public void setN3(long n3) {
            N3 = n3;
        }

        public void setC0(long c0) {
            C0 = c0;
        }

        public void setC1(long c1) {
            C1 = c1;
        }

        public void setC2(long c2) {
            C2 = c2;
        }

        public String toString() {
            return "N1: "+ N1 + " N2: " + N2 + " N3: " + N3 + " C0: " + C0 + " C1: " + C1 + " C2: " + C2;
        }

        public double getProb() {
            double k2 = (Math.log((double) N2 + 1)+1)/(Math.log((double) N2 + 1) + 2);
            double k3 = (Math.log((double) N3 + 1)+1)/(Math.log((double) N3 + 1) + 2);
            return (double) ((k3 * ((double) N3 / C2)) + 
                 (1 - k3) * (k2 * ((double) N2 / C1)) + 
                 (1 - k3) * (1 - k2) * ((double) N1 / C0));

        }
    }

    public static class LineReducerClass extends Reducer<Triplet,Text,Text,Text> {
        Long c1 = 0L;
        Long c2 = 0L;

        Long total = 0L;
        

        public void setup(Context context) {
            total = Long.parseLong(context.getConfiguration().get("total"));
        }
        
        @Override
        public void reduce(Triplet key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            Long sum = 0L;
            if (key.getCount() == 1) {
                for (Text val : values) {
                    sum += Long.parseLong(val.toString());
                }
                c1 = sum;
            } else if (key.getCount() == 2) {
                for (Text val : values) {
                    sum += Long.parseLong(val.toString());
                }
                c2 = sum;
            } else if (key.getCount() == 3) {
                Long[] stats = new Long[4];
                for (int i = 0; i < stats.length; i++) {
                    stats[i] = 0L;
                }
                for (Text val : values) {
                    System.out.println(val.toString());
                    String [] temp = val.toString().split(" ");
                    for(int i = 0; i < stats.length; i++) {
                        stats[i] = stats[i] + Long.parseLong(temp[i].split(":")[1]);
                    }
                }
                LineProb lp = new LineProb( key.toString() 
                , stats[0]
                , stats[1]
                , stats[2]
                , total
                , c1, c2);
                context.write(new Text (key.toString()) , new Text(Double.toString(lp.getProb())));
            }   //w2w1w3
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


    public static Long downloadTotal(String bucketName) {
        Long total = 0L;
        try {
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
            .withRegion("us-east-1")
            .build();
            S3Object object = s3.getObject(bucketName , "total.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            String line;
            while ((line = reader.readLine()) != null) {
                total += Long.parseLong(line);
            }
            reader.close();
        } catch (Exception e) {
            System.err.println("Error occurred while reading from S3: " + e.getMessage());
            e.printStackTrace();
        }
        return total;
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        String bucketName = args[1];
        Configuration conf = new Configuration();
        conf.set("total", downloadTotal(bucketName).toString());
        conf.set("bucketName", bucketName);
        Job job = Job.getInstance(conf, "step2");
        job.setJarByClass(step2.class);
        
        job.setMapperClass(LineMapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(LineReducerClass.class);

        job.setMapOutputKeyClass(Triplet.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        TextInputFormat.addInputPath(job,  new Path("s3://" + bucketName + "/output_step1"));
        FileOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output_step2"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
