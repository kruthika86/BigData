import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



import java.util.regex.Pattern;
import java.util.regex.Matcher;


public class BusinessLocation {
	public static class LocationMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			Text category = new Text();
			String input = value.toString();
			String[] fields = input.split("::");
		    String expPattern = "List\\((.*)\\)";
		    Pattern r = Pattern.compile(expPattern);
		    Matcher m ;
		    String[] categories;
			if (fields.length == 3) {
				if (fields[1].contains("Palo Alto")) {
					m = r.matcher(fields[2].trim());
					if(m.find()){
						categories = m.group(1).split(",");
						NullWritable noValue = NullWritable.get();
						for(String categoryString: categories){
							category.set(categoryString.trim());
							context.write(category, noValue);
						}
					}
				
				}	
			}
		}
	}
	public static class LocationReduce
	extends Reducer<Text,NullWritable,Text,NullWritable> {
		private NullWritable finalResult = NullWritable.get();
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context
				) throws IOException, InterruptedException {
			context.write(key, finalResult); // create a pair <keyword, number of occurences>
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		
		// Evaluate the hadoop console args 
		if (remainingArgs.length != 2) {
			System.err.println("Usage: List PaloAlto BusinessId <in> <out>");
			System.exit(2);
		}

		// create a job with name "Business_Ids"
		Job job = new Job(conf, "Business_Ids");
		job.setJarByClass(BusinessLocation.class);
		job.setMapperClass(LocationMapper.class);
		job.setReducerClass(LocationReduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(NullWritable.class);
		
		//set  HDFS path of the input 
		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		// set  HDFS path for  output
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
		
		//Wait for job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
