import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA=relation
			String[] line = value.toString().trim().split("\t");
			context.write(new Text(line[0]), new Text(line[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: user,movie,rating
			HashSet<String> set = new HashSet<String>();
			String[] user_movieRatings = value.toString().trim().split("\t");
			if (user_movieRatings.length != 2)
				return;
			String[] movies = user_movieRatings[1].split(",");
			double sum = 0.0;
			for (int i = 0; i < movies.length; i++) {
				String[] movie = movies[i].trim().split(":");
				context.write(new Text(movie[0]), new Text(user_movieRatings[0] + ":" + movie[1]));
				set.add(movie[0]);
				sum += Double.parseDouble(movie[1]);
			}
			sum /= movies.length;
			for (int i = 1; i <= 5; i++) {
				if (!set.contains(String.valueOf(i)))
					context.write(new Text(String.valueOf(i)), new Text(user_movieRatings[0] + ":" + sum));
			}
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//key = movieB
			//value = <movieA=relation, movieC=relation, userA:rating, userB:rating...>
			Map<String, Double> relationMap = new HashMap<String, Double>();
			Map<String, Double> ratingMap = new HashMap<String, Double>();
			for (Text value: values) {
				if(value.toString().contains("=")) {
					String[] movie_relation = value.toString().split("=");
					relationMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
				}
				else {
					String[] user_rating = value.toString().split(":");
					ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
				}
			}
			for (String movie : relationMap.keySet()) {
				double relation = relationMap.get(movie);
				for (String user : ratingMap.keySet()) {
					double rating = ratingMap.get(user);
					String outputKey = user + ":" + movie;
					double outputValue = relation * rating;
					context.write(new Text(outputKey), new DoubleWritable(outputValue));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);
		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);
		job.setReducerClass(MultiplicationReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));	
		job.waitForCompletion(true);
	}
}
