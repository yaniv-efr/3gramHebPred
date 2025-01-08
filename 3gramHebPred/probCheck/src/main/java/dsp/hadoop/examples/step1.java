package steps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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

public class step1 {

    public static class Triplet implements WritableComparable<Triplet> {
        public String w1 = "*";
        public String w2 = "*";
        public String w3 = "*";
        public int count = 0;

        public Triplet() {
        }

        public Triplet(String w) {
            String[] words = w.split(" ");
            if (words.length == 3) { //w3 w2 w1
                w1 = words[2];
                w2 = words[1];
                w3 = words[0];
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
            return w3 + " " + w2 + " " + w1;
        }

        public Boolean equals(Triplet t) {
            return w1.equals(t.w1) && w2.equals(t.w2) && w3.equals(t.w3);
        }

        public int getCount() {
            return count;
        }
    }


    public static class LineMapperClass extends Mapper<LongWritable, Text, Triplet, LongWritable> {
        List<String> stopwords = new ArrayList<String>();
        Map<String, Long> lastNonWritten = new HashMap<String, Long>();

        public ArrayList<String> loadStopWords(Context context) {
            ArrayList<String> stopwords = new ArrayList<String>();
            try {
                AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion("us-east-1")
                .build();
                String bucketName = context.getConfiguration().get("bucketName");
                BufferedReader reader = new BufferedReader(new InputStreamReader(s3.getObject(bucketName, "input/stop.txt").getObjectContent()));

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


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            
            String[] ngram = value.toString().split("\t"); //w1w2w3
            String[] ngramWords = ngram[0].split(" ");
            for (String word : ngramWords) {
                if (stopwords.contains(word) || word.charAt(0) == '"' || word.charAt(0) == '.' || word.charAt(word.length() - 1) == '"' || word.charAt(word.length() - 1) == '.') {
                    System.out.println("stopword: " + word);
                    return;
                }
            }
            if (ngram[0].split(" ").length >= 3) {
                Triplet t = new Triplet(ngram[0]);
                context.write(t, new LongWritable(Long.parseLong(ngram[2])));//w3 w2 w1
                context.write(new Triplet(ngram[0].split(" ")[2]), new LongWritable(Long.parseLong(ngram[2])));//w3
                context.write(new Triplet(ngram[0].split(" ")[1] + " " + ngram[0].split(" ")[2]), new LongWritable(Long.parseLong(ngram[2])));//w2 w3
            }

            if (ngram[0].split(" ").length >= 2) {
                String duo = ngram[0].split(" ")[0] + " " + ngram[0].split(" ")[1];    //w1w2
                Triplet t1 = new Triplet(duo);
                context.write(t1, new LongWritable(Long.parseLong(ngram[2])));
                context.write(new Triplet(ngram[0].split(" ")[1]), new LongWritable(Long.parseLong(ngram[2])));//w2
            }
            String first = ngram[0].split(" ")[0];   //w1
            Triplet t3 = new Triplet(first);
            context.write(t3, new LongWritable(Long.parseLong(ngram[2])));
            context.write(new Triplet(), new LongWritable(Long.parseLong(ngram[2])));
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

        public void setN1(long n1) {
            N1 = n1;
        }

        public void setN2(long n2) {
            N2 = n2;
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

        public String toString() {
            return line + "\t" + "N1:"+ N1 + " N2:" + N2 + " N3:" + N3 + " C0:" + C0 + " C1:" + C1 + " C2:" + C2;
        }

        public String reverseLine() {
            String[] words = line.split(" ");
            if (words.length == 3) {
                return words[2] + " " + words[1] + " " + words[0];
            } else if (words.length == 2) {
                return words[1] + " " + words[0];
            } else {
                return words[0];
            }
        }

        public double getProb() {
            double k2 = (Math.log((double) N2 + 1)+1)/(Math.log((double) N2 + 1) + 2);
            double k3 = (Math.log((double) N3 + 1)+1)/(Math.log((double) N3 + 1) + 2);
            return (double) ((k3 * (N3 / C2)) + (1 - k3) * (k2 * (N2 / C1)) + (1 - k3) * (1 - k2) * (N1 / C0));
        }
    }

    public static class LineReducerClass extends Reducer<Triplet,LongWritable,LongWritable,Text> {
        LineProb lp;
        long id = 0;

        @Override
        public void setup(Context context) {
            lp = LineProb.start(null, 0);
        }
        
        @Override
        public void reduce(Triplet key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            if (key.isAstrix()) {
                Long total = (long) sum;
                uploadTotal(total , context); //upload total to s3
            } else if (key.getCount() == 1) {
                lp = LineProb.start(key.toString(), 0);
                lp.setN1(sum);
            } else if (key.getCount() == 2) {
                lp.setN2(sum);
            } else if (key.getCount() == 3) {
                context.write(new LongWritable(id++), new Text(LineProb.dupeN3(lp, key.toString(), sum).toString()));
            }   
        }

        public void uploadTotal(long total , Context context) {
            try {
                AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion("us-east-1")
                .build();
                String bucketName = context.getConfiguration().get("bucketName");   
                s3.putObject(bucketName, "total.txt", Long.toString(total));
            } catch (Exception e) {
                System.err.println("Error occurred while writing to S3: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }



    public static class LineCombiner extends Reducer<Triplet,LongWritable,Triplet,LongWritable> {
        
        @Override
        public void reduce(Triplet key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Triplet, LongWritable> {
        @Override
        public int getPartition(Triplet key, LongWritable value, int numPartitions) {
        // Ensure the partition is based only on w3
            if (key.isAstrix()) {
                return 0;
            } else {
                return (key.w1.hashCode() & Integer.MAX_VALUE) % numPartitions;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        if (args.length != 2) {
            System.err.println("Usage: App <bucket-name>");
            for (String arg : args) {
                System.err.println(arg);
            }
            System.exit(1);
        }
        String bucketName = args[1];
        Configuration conf = new Configuration();
        conf.set("bucketName", bucketName);
        Job job = Job.getInstance(conf, "step1");
        job.setJarByClass(step1.class);
        
        job.setMapperClass(LineMapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(LineReducerClass.class);
        job.setCombinerClass(LineCombiner.class);

        job.setMapOutputKeyClass(Triplet.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        TextInputFormat.addInputPath(job,  new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        FileOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output_step1"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}