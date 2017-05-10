import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.util.HashSet;
import java.util.Set;

public class Rating {

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		public static Set<String> set = new HashSet<String>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws
		InterruptedException, IOException {
			Configuration configuration = context.getConfiguration();
			String fileLocation = configuration.get("intermediateFile");
			FileSystem fs = FileSystem.get(new Configuration());
			FSDataInputStream inputStream = fs.open(new Path(fileLocation));
			BufferedReader bufferData = new BufferedReader(new InputStreamReader(inputStream));
			String inputLine = bufferData.readLine();
			while (inputLine != null) {
				String[] columnSplit = inputLine.split("::");
				if (columnSplit.length == 3 && columnSplit[1].contains("Stanford,")) {
					set.add(columnSplit[0].trim());
				}
				inputLine = bufferData.readLine();
			}
		}

		@Override
		protected void map(LongWritable key, Text line,
				Mapper<LongWritable, Text, Text, Text>.Context context)
						throws IOException, InterruptedException {

			Text user_id = new Text();
			Text review_text = new Text();
			String [] columnSplit = line.toString().split("::"); 
			if (columnSplit.length == 4 && set.contains(columnSplit[2].trim())) {
				String userId = columnSplit[1].trim();
				String reviews = columnSplit[3].trim();
				context.write(new Text(userId), new Text(reviews));
			}
		}
	}
	public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException, URISyntaxException {

		Configuration configuration = new Configuration();
		String[] hadoopArgs = new GenericOptionsParser(configuration, args)
				.getRemainingArgs();
		if (hadoopArgs.length != 3) {
			System.err.println("Usage: BusinessLocation <in> <out>");
			System.exit(2);
		}

		Path inputFile = new Path(hadoopArgs[1]);
		Path outputFile = new Path(hadoopArgs[2]);
		configuration.set("intermediateFile", hadoopArgs[0]);

		// create job with name "In_Memory_Join"
		Job job = new Job(configuration, "In_Memory_Join");
		job.setJarByClass(Rating.class);
		job.setMapperClass(RatingMapper.class);
		job.setNumReduceTasks(0);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);				
		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);
		FileInputFormat.setMinInputSplitSize(job, 500000000);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
