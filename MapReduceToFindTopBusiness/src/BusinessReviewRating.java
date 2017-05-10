import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


class DataValueComparator implements Comparator<Object> {
	 
	Map<String, Float> map = new HashMap<String, Float>();
 
	public DataValueComparator(Map<String, Float> map) {
		this.map = map;
	}
	
	public int compare(Object keyA, Object keyB) {
		
		Float valueA= (Float) map.get(keyA);
		Float valueB= (Float) map.get(keyB);
		int compareResult=valueB.compareTo(valueA);
		if(compareResult>=0)
			return 1;		
		return -1;
	}
}	

public class BusinessReviewRating {
	public static class TopTenBusinessRatingMapper extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
            FloatWritable rating = new FloatWritable();
			Text businessId = new Text();
			String inputData = value.toString();
			String[] columns = inputData.split("::");
		    if (columns.length == 4) {
		    	    businessId.set(columns[2]);
					rating.set(Float.parseFloat(columns[3]));	
					context.write(businessId, rating);
				}	
			}
	}
	public static class TopTenBusinessRatingReduce
	extends Reducer<Text,FloatWritable,Text,FloatWritable> {
		private FloatWritable averageRating = new FloatWritable();
		static Map<String, Float> hashmap = new HashMap<String, Float>();
		static Map<String, Float> map = new TreeMap<String, Float>(new DataValueComparator(hashmap));

		public void reduce(Text businessId, Iterable<FloatWritable> values,
				Context context
				) throws IOException, InterruptedException {
			float sum = 0.0f;
			int count = 0;
			for(FloatWritable rating: values){
				sum += rating.get();
				count++;
			}
		    float avg = sum/count;
			averageRating.set(avg);
			hashmap.put(businessId.toString(), avg);
			
		}
		@Override
		protected void cleanup(
				Reducer<Text,FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			map.putAll(hashmap);
			int i = 0;
			for (Map.Entry<String, Float> entry : map.entrySet()) {
				context.write(new Text(entry.getKey()),
						new FloatWritable(entry.getValue()));
				i++;
				if (i == 10)
					break;
			}
			
		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// get  args
		if (remainingArgs.length != 2) {
			System.err.println("Usage: TopTenBusinessRating <in> <out>");
			System.exit(2);
		}

		// create job with name "TopTenBusinessRating"
		Job job = new Job(conf, "TopTenBusinessRating");
		job.setJarByClass(BusinessReviewRating.class);
		job.setMapperClass(TopTenBusinessRatingMapper.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(TopTenBusinessRatingReduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(FloatWritable.class);
		
		//set HDFS path of input 
		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		// set HDFS path for output
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
		
		//Wait for job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
