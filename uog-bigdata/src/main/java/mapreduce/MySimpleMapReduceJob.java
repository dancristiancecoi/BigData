package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MySimpleMapReduceJob extends Configured implements Tool {

	// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyMapper extends Mapper<Object, Text, ArticlePageRankWritable, Text> {
		
		ArticlePageRankWritable articlePageRank = new ArticlePageRankWritable();
//		Text article = new Text();
		Text outlink = new Text();
	
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}

		// The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] lines = value.toString().split("/n");
			
			for ( String line : lines){
				
				if (line.contains("REVISION")) {
					String[] lineSplitted = line.split(" ");
					ArticlePageRankWritable newKey = new ArticlePageRankWritable();
					newKey.setArticle(lineSplitted[3]);
					newKey.setPageRank(1.0);
					this.articlePageRank = newKey;
				}
				
				if (line.contains("MAIN")) {
					String[] lineSplitted = line.split(" ");
					ArrayList<String> outlinks = new ArrayList<String>(Arrays.asList(lineSplitted));
					outlinks.remove(0);
					if (lineSplitted.length == 1) {
						this.outlink.set("");
						context.write(articlePageRank, this.outlink);
					}
					
					for(int i = 1; i<lineSplitted.length; i++) {
						String outlink = lineSplitted[i];
						
						if (articlePageRank.getArticle().compareTo(outlink)!= 0) {
							this.outlink.set(outlink);
							context.write(articlePageRank, this.outlink);						
						}
					}
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyReducer extends Reducer<ArticlePageRankWritable,Text, ArticlePageRankWritable, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}
		
		// The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		// Make sure that the output key/value classes also match those set in your job's configuration (see below).
		@Override
		protected void reduce(ArticlePageRankWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// ...
//			ArrayList<Text> outlinks = new ArrayList<Text>();
			StringBuilder outlinks = new StringBuilder();
			
			for(Text outlink : values) {
				if(!outlinks.toString().contains(outlink.toString())) {
					outlinks.append(outlink.toString()).append(",");
				}
			}
			context.write(key, new Text(outlinks.toString()));
			
//			if (!outlinks.isEmpty()) {
//				String[] outlinksList = new String[outlinks.size()];
//				outlinks.toArray(outlinksList);
//				context.write(key, new ArrayWritable(outlinksList));
//				
//			}

		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "MySimpleMapReduceJob");
		job.setJarByClass(MySimpleMapReduceJob.class);
//		
//		job.setInputFormatClass(NLineInputFormat.class);
//		NLineInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(ArticlePageRankWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.setNumReduceTasks(4);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MySimpleMapReduceJob(), args));
	}
}
