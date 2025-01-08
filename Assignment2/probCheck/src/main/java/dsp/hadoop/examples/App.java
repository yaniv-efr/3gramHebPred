import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class App {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 9;

    public static void deleteS3pre(String prefix , String bucketName){
        try {

                // List all objects under the given path
                ListObjectsV2Request listRequest = new ListObjectsV2Request()
                        .withBucketName(bucketName)
                        .withPrefix(prefix); // Prefix for the "directory"

                ListObjectsV2Result result;
                do {
                        result = S3.listObjectsV2(listRequest);

                        // Delete all listed objects
                        result.getObjectSummaries().forEach(object -> {
                                S3.deleteObject(bucketName, object.getKey());
                        });

                        // Continue listing if there are more objects
                        listRequest.setContinuationToken(result.getNextContinuationToken());
                } while (result.isTruncated());

                System.out.println("Deleted all objects under " + prefix);
        } catch (Exception e) {
                System.err.println("Error occurred while deleting from S3: " + e.getMessage());
                e.printStackTrace();
        }
    }

    public static void main(String[]args){

        if (args.length != 1) {
            System.err.println("Usage: App <bucket-name>");
            System.exit(1);
        }
        String bucketName = args[0];
        System.out.println("Bucket name: " + bucketName);
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to aws");
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        System.out.println( "list cluster");
        System.out.println( emr.listClusters());

        deleteS3pre("output_step" , bucketName);
        deleteS3pre("j-" , bucketName);
        deleteS3pre("total" , bucketName);
        

        // Step 1
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/jars/step1.jar")
                .withMainClass("Step1.step1")
                .withArgs(bucketName);

        // Step 2
        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/jars/step2.jar")
                .withMainClass("Step2.step2")
                .withArgs(bucketName);

        // Step 3
        HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/jars/step3.jar")
                .withMainClass("Step3.step3")
                .withArgs(bucketName);


        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        

        //step 2 should run after step 1 finishes
        StepConfig stepConfig2 = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(step2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig3 = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(step3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Job flow
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("3gram")
                .withInstances(instances)
                .withSteps(stepConfig1, stepConfig2, stepConfig3)
                .withLogUri("s3://" + bucketName + "/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
