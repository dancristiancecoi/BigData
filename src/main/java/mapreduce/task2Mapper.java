package mapreduce;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class task2Mapper extends Mapper<Object, Text, Text, IntWritable>{
	
	private Text selectedLine = new Text();
	private IntWritable articleNumber = new IntWritable();
	
    protected void map(Object key, Text value,
            Context context)
            throws IOException, InterruptedException {
    	String line = value.toString();
    	if( line.contains("REVISION")) {
    		String[] lineSplitted = line.split(" ");
    		selectedLine.set(lineSplitted[1]);
    		articleNumber.set(1);
    		context.write(selectedLine, articleNumber);
    			
    	}
    	
    	

    }

}
