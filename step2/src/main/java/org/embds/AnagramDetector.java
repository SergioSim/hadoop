package org.embds;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AnagramDetector {
	
	public static class AnagramDetectorMap extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String word = value.toString();
			char[] characters = word.toLowerCase().toCharArray();
			Arrays.sort(characters);
			context.write(new Text(String.valueOf(characters)), new Text(word));
		}
	}
	
	public static class AnagramDetectorReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> anagrams = values.iterator();
			String anagamsList = anagrams.next().toString();
			boolean isAtLeastTwo = false;
			while(anagrams.hasNext()) {
				anagamsList += ", " + anagrams.next().toString();
				isAtLeastTwo = true;
			}
			if(isAtLeastTwo) {
				context.write(key, new Text(anagamsList));
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] ourArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "Anagram detector job v1.0");
		
		job.setJarByClass(AnagramDetector.class);
		job.setMapperClass(AnagramDetectorMap.class);
		job.setReducerClass(AnagramDetectorReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(ourArgs[1]));
		
		if(job.waitForCompletion(true)) {
			System.exit(0);
		}
		System.exit(1);
	}
}
