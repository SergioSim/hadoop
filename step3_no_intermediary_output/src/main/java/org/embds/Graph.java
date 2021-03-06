package org.embds;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Graph {
	
	public enum GRAPH_COUNTERS {
		OUTPUT_NB_NONBLACK
	};
	
	public static class GraphMap extends Mapper<Text, Text, Text, Text> {
		
		private static final String GREY = "GREY";
		
		public void map(Text key, Text node, Context context) throws IOException, InterruptedException {
			String[] parts = node.toString().split("\\|");
		    if(parts.length!=3) return;
		    String[] neighbours = parts[0].split(",");
		    String colour = parts[1];
		    int depth = -2;
		    try {
		      depth=Integer.parseInt(parts[2]);
		    } catch(Exception e) {
		      depth = -2;
		    }
		    if(depth == -2) return;
		    
			if(colour.equals(GREY)) {
				for(int i=0; i<neighbours.length; ++i) {
			        if(neighbours[i].equals("")) continue;
					context.write(new Text(neighbours[i]), new Text("|GREY|" + Integer.toString(depth+1)));
				}
				context.write(key, new Text(parts[0]+"|BLACK|"+Integer.toString(depth)));
			} else {
				context.write(key, node);
			}
		}
	}
	
	public static class GraphReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			// When isShortest is true we compute the shortest route, else - the longest
			boolean isShortest = conf.getBoolean("org.embds.shortest", false);
			int max_depth = -1;
			int min_depth = -1;
			// When all nodes are gray or black, we have already the shortest route.
			// In this case we keep the min_depth instead of max_depth.
			boolean all_nodes_gray_or_black = true;
			String new_neighbours = "";
			String new_color = "";

			Iterator<Text> i = values.iterator();
			while(i.hasNext()) {
		      Text node = i.next();
		      int depth = -2;
		      String neighbours = "";
		      String colour = "";
		      String[] parts = node.toString().split("\\|");
		      if(parts.length!=3) continue;
		      neighbours = parts[0];
		      colour = parts[1];
		      if(colour.equals("WHITE")) all_nodes_gray_or_black = false;
		      try {
		        depth = Integer.parseInt(parts[2]);
		      } catch(Exception e) {
		        depth = -2;
		      }
		      if(depth == -2) continue;
		      if(depth > max_depth) max_depth = depth;
		      
		      if(depth < min_depth || min_depth == -1) min_depth = depth;
		      if(neighbours.length() > new_neighbours.length()) new_neighbours = neighbours;
		      if(new_color.equals("") || (
		    		  (new_color.equals("WHITE") && (colour.equals("GREY") || colour.equals("BLACK"))) ||
		              (new_color.equals("GREY") && (colour.equals("BLACK")))
		    		  )) {
		        new_color = colour;
		      }
			}
			
			if(all_nodes_gray_or_black && isShortest) max_depth = min_depth;

		    if(!new_color.equals("BLACK")) context.getCounter(Graph.GRAPH_COUNTERS.OUTPUT_NB_NONBLACK).increment(1);
			context.write(key, new Text(new_neighbours+"|"+new_color+"|"+max_depth));
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] ourArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// /graph_input.txt
		String input_path = ourArgs[0];
		// /ourGraphOutput
		String output_path_prefix = ourArgs[1];
		String output_path = "";
		int nb_step = 0;
		long nb_nodes_non_black = 0;
		
		boolean isShortest = false;
		boolean noIntermediaryOutput = false;
		for (int i = 2; i <= 3; i++) {
			if(ourArgs.length > i && ourArgs[i].equals("--shortest")) {
				isShortest = true;
			}
			if(ourArgs.length > i && ourArgs[i].equals("--no-intermediary-output")) {
				noIntermediaryOutput = true;
			}
		}
		conf.setBoolean("org.embds.shortest", isShortest);
		while(true) {
			if(nb_step>0) {
				input_path = output_path + "/part-r*";
			
			    if(nb_nodes_non_black == 0) {
			      break;
				}
			}
			nb_step = nb_step + 1;
			// /ourGraphOutput-step-1
			output_path = output_path_prefix + "-step-" + nb_step;
			
			System.out.println("Execution cycle #" + nb_step + ": input '" + input_path + "', output '" + output_path + "'");
			
			Job job=Job.getInstance(conf, "Graph traversal v1.0");
			
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			job.setJarByClass(Graph.class);
			job.setMapperClass(GraphMap.class);
			job.setReducerClass(GraphReduce.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(input_path));
			FileOutputFormat.setOutputPath(job, new Path(output_path));
			
			if(!job.waitForCompletion(true)) {
				System.out.println("ERROR: execution cycle #" + nb_step + " failed.");
				System.exit(-1);
			}
			
			Counters cn = job.getCounters();
			Counter c1 = cn.findCounter(GRAPH_COUNTERS.OUTPUT_NB_NONBLACK);
			if(c1!=null) {
				nb_nodes_non_black = c1.getValue();
			}
		}
		if(noIntermediaryOutput) {
			FileSystem fs = FileSystem.get(conf);
			// the “output directory” command line argument should be the name of the final, single output directory
			fs.rename(new Path(output_path), new Path(output_path_prefix));
			// not conserving all intermediary output directories (step-1, step-2, etc.)
			FileStatus[] list = fs.globStatus(new Path(output_path_prefix + "-step-[0-9]*"));
			for(int i=0; i<list.length; ++i) {
				fs.delete(list[i].getPath(), true);
			}
			System.out.println("All nodes processed; final output directory: '" + output_path_prefix + "'");
		} else {
			System.out.println("All nodes processed; final output directory: '" + output_path + "'");
		}
		System.exit(0);

	}
}
