package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

	public int run(String[] args) throws Exception {
//		Job job = Job.getInstance(getConf(), "PageRank");
//		job.setJarByClass(MySimpleMapReduceJob.class);
//		job.setMapperClass(PageRankMapReduce1.Mapper1.class);
//		job.setReducerClass(PageRankMapReduce1.Reducer1.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		job.setNumReduceTasks(4);
//
//		return (job.waitForCompletion(true) ? 0 : 1);
		
		
		PageRank pageRank = new PageRank();
		boolean isCompleted = pageRank.job1(args[0], args[1]);
	    if (!isCompleted) {
            System.exit(1);
        }
	    
	    isCompleted = pageRank.job2(args[1], args[1]+"/job2");
    
	    if (!isCompleted) {
	    	System.exit(1);
        }
	   		
		isCompleted = pageRank.job2(args[1]+"/job2", args[1]+"/job3");
		isCompleted = pageRank.job2(args[1]+"/job3", args[1]+"/job4");
		isCompleted = pageRank.job2(args[1]+"/job4", args[1]+"/job5");
		
		
		return (isCompleted ? 0 : 1);

		
	}
	
	
//	public boolean setupJob(String in, String out) throws IOException, 
//	ClassNotFoundException, InterruptedException {
//
//		Job job = Job.getInstance(getConf(), "PageRank");
//		job.setJarByClass(MySimpleMapReduceJob.class);
//		job.setMapperClass(PageRankMapReduce1.Mapper1.class);
//		job.setReducerClass(PageRankMapReduce1.Reducer1.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		FileInputFormat.addInputPath(job, new Path(in));
//		FileOutputFormat.setOutputPath(job, new Path(out));
//		job.setNumReduceTasks(4);
//		
//		return job.waitForCompletion(true);
//	}
	
	public boolean job1(String in, String out) throws IOException, 
	ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration(), "job1");
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

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
	}
}
