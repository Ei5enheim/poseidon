/*
 * Author: Gopidi Rajesh
 * File Name: SOMTrainingTest.java 
 * Course: COMP790-042
 * Assignment: Final Project
 *
 */

import java.io.*;
import java.util.*;
import java.net.*;

import poseidon.hadoop.io.*;
import poseidon.packet.*;
import poseidon.hadoop.mapreduce.*;
import poseidon.hadoop.mapreduce.lib.input.*;
import poseidon.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;

public class SOMTrainingTest
{
	public static void addCacheFiles (String path, Job job, int iteration) throws IOException
	{
		Path dir = new Path(path);
		FileSystem fs = FileSystem.get(dir.toUri(),
					       job.getConfiguration());
		StringBuilder str;
		FileStatus[] status;
		if (iteration ==0) {
		    status = fs.listStatus(dir);
		} else {
		    status = fs.listStatus(dir, new PathFilter() {
					public boolean accept (Path path)
					{
						if (path.toString().matches(".*[0-9]+-[0-9]+-[0-9]+.*"))
							return (true);
						
						return (false);
					}
				});
		}
	 	Path file;	
		System.out.println("got the files");
		for (int i = 0; i < status.length; i++) {
			file = status[i].getPath();
			//if (file.toString().matches(".*temporary.*"))
				//continue;

			DistributedCache.addCacheFile(file.toUri(),
						      job.getConfiguration());
		}	

	}

	public static void main (String args[]) throws Exception
	{
		int totalEpochs = 200; 
		int timeConstant = 200;
		long startTime = 0, finishTime = 0;
		boolean exitStatus = false;
		String defaultOutputPath =  args[2];
		String outputPath;
		String inputPath;

		if (args.length < 2) {
			System.err.println("In sufficient arguments \n");
			System.exit(-1);
		}
		for (int i = 0; i < totalEpochs; i++) {

			//Configuration conf = new Configuration(); 
			Job job = new Job(new Configuration());
			job.setJarByClass(SOMTrainingTest.class);
			job.setJobName("Training of SOM-iteration:" + Integer.toString(i+1));
			//DistributedCache.createSymlink(job.getConfiguration());
			inputPath = args[2] + "/" + "iteration"+ Integer.toString(i);
			System.out.println(inputPath);
			addCacheFiles(inputPath, job, i);
			System.out.println("here");
			DistributedCache.createSymlink(job.getConfiguration());
			job.setInputFormatClass(PCAPFileInputFormat.class);
			PCAPFileInputFormat.addInputPath(job, new Path(args[0]));
			PCAPFileInputFormat.addInputPath(job, new Path(args[1]));
			job.setOutputFormatClass(SOMFileOutputFormat.class);
			outputPath = defaultOutputPath + "/" + "iteration"+ Integer.toString(i+1);
			SOMFileOutputFormat.setOutputPath(job, new Path(outputPath));
			job.setMapperClass(SOMTraining.SOMMapper.class);
			job.setCombinerClass(SOMTraining.SOMCombiner.class);
			job.setReducerClass(SOMTraining.SOMReducer.class);
			job.setMapOutputKeyClass(GridKey.class);
			job.setMapOutputValueClass(BatchSOMUpdateWritable.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(NeuronVectorWritable.class);
			job.setReduceSpeculativeExecution(true); 
			job.setNumReduceTasks(80);
			job.getConfiguration().setInt("sigma", 4);
			job.getConfiguration().setInt("IterationNumber", i+1);
			job.getConfiguration().setInt("X Dimension", 12);
			job.getConfiguration().setInt("Y Dimension", 8);
			job.getConfiguration().setInt("TimeConstant", timeConstant);
			job.getConfiguration().setInt("totalIterations", totalEpochs);
			
			System.out.println("********** Starting the job now *********");
			//job.setNumReduceTasks(1.75 * nodes * mapred.tasktracker.reduce.tasks.maximum);
			startTime = new Date().getTime();
			exitStatus = job.waitForCompletion(true);
			finishTime = new Date().getTime();
			System.out.println("Time elapsed : "+ (finishTime - startTime));
			System.out.println("************** The END ****************");
			if ((i == totalEpochs-1)|| (!exitStatus))
				System.exit(exitStatus ? 0 : 1); 
		}
	}
}
