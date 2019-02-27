package mapreduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {
	
	public static String INPUT_PATH = "";
	public static String OUTPUT_PATH = "";
	public static int ITERATIONS = 5;
	public long TIMESTAMP=0;

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 * 
	 * This function calls and runs all the jobs required to run the PageRank algorithm.
	 */
	public int run(String[] args) throws Exception {
		
		//  check for valid parameters (needs to be changed to 4 when Date/Time filter implemented)
		if (args.length!=4) {
			System.out.println("\n ERROR: Incorrect number of parameters have been supplied \n");
			System.exit(0);
		}
		
		try {
			PageRank.INPUT_PATH = args[0];
			PageRank.OUTPUT_PATH = args[1];
			PageRank.ITERATIONS = Integer.parseInt(args[2]);
			TIMESTAMP = utils.ISO8601.toTimeMS(args[3]);
		}
		catch (Exception e) {
			System.out.println("\n ERROR: Invalid parameters have been supplied \n");
			System.exit(0);
		}
		
        // delete output path if it exists already
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(PageRank.OUTPUT_PATH)))
            fs.delete(new Path(PageRank.OUTPUT_PATH), true);
        
        PageRank pageRank = new PageRank();
        NumberFormat nf = new DecimalFormat("00");
        
        System.out.println("\n RUNNING JOB #1: PARSING INPUT DATA \n");
		boolean isCompleted = job1(PageRank.INPUT_PATH, PageRank.OUTPUT_PATH + "/iter00", TIMESTAMP);
	    if (!isCompleted) {
	    	System.out.println("\n JOB #2 ENCOUNTERED AN ERROR \n");
            System.exit(1);
        }
	    
	    for (int i=0; i < PageRank.ITERATIONS; i++) {
	    	String inputPath = PageRank.OUTPUT_PATH + "/iter" + nf.format(i);
	    	String outputPath = PageRank.OUTPUT_PATH + "/iter" + nf.format(i+1);
	    	System.out.println("\n RUNNING JOB #2: PAGERANK ITERATION " + (i+1) + " OUT OF " + PageRank.ITERATIONS + "\n");
	    	isCompleted = job2(inputPath, outputPath);
	    	
	    	// The program encountered an error before completing the loop
	    	if (!isCompleted) {
	    		System.out.println("\n JOB #2 ENCOUNTERED AN ERROR \n");
		    	System.exit(1);
	        }
	    } 
	    String inputPath = PageRank.OUTPUT_PATH + "/iter" + nf.format(PageRank.ITERATIONS);
	    String outputPath = PageRank.OUTPUT_PATH + "/finalOutput";
	    isCompleted = job3(inputPath, outputPath);
		return (isCompleted ? 0 : 1);		
	}
	
	
	/**	
	 * This is the first job run by HADOOP.
	 * It will parse through the input file and extract
	 * all information needed to perform the PageRank algorithm.
	 * 
	 * @param in - The input file directory path
	 * @param out - The output file directory path
	 * @return boolean - Did the job successfully complete
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public boolean job1(String in, String out, Long timestamp) throws IOException, 
	ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
	    conf.set("textinputformat.record.delimiter", "\n\n");
	    conf.set("instance_of_timestamp", Long.toString(this.TIMESTAMP));
		Job job = Job.getInstance(conf, "job1");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(PageRankMapReduce1.Mapper1.class);
		job.setReducerClass(PageRankMapReduce1.Reducer1.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setNumReduceTasks(4);
		return job.waitForCompletion(true);
	}
	
	/**
	 * This is the second job run by HADOOP.
	 * This job is to be run iteratively, calculating the ranking.
	 * The higher th	e number of iterations the more accurate the ranking.
	 * 
	 * @param in - The input file directory path
	 * @param out - The output file directory path
	 * @return boolean - Did the job successfully complete
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public boolean job2(String in, String out) throws IOException, 
	ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(new Configuration(), "job2");
		job.setJarByClass(PageRank.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(PageRankMapReduce2.Mapper2.class);
		job.setReducerClass(PageRankMapReduce2.Reducer2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setNumReduceTasks(4);
		return job.waitForCompletion(true);
	}
	
	public boolean job3(String in, String out) throws IOException, 
	ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(new Configuration(), "job3");
		job.setJarByClass(PageRank.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(PageRankMapReduce3.Mapper3.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
	}
}
