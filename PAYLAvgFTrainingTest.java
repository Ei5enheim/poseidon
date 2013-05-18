/*
 * Author: Gopidi Rajesh
 * File Name: PAYLAvgFTrainingTest.java 
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

public class PAYLAvgFTrainingTest
{
	public static void addCacheFiles (String inputPath, Job job) throws IOException
	{
		Path file;
		FileStatus[] status;

		Path dir = new Path(inputPath);
		FileSystem fs = FileSystem.get(dir.toUri(),
					       job.getConfiguration());
		status = fs.listStatus(dir, new PathFilter() {
				public boolean accept (Path path)
				{
				if (path.toString().matches(".*[0-9]+-[0-9]+-[0-9]+.*"))
				return (true);

				return (false);
				}
				});
		System.out.println("got the files");
		for (int i = 0; i < status.length; i++) {
			file = status[i].getPath();
			DistributedCache.addCacheFile(file.toUri(),
					job.getConfiguration());
		}	
	}

	public static void main (String args[]) throws Exception
	{
		long startTime = 0, finishTime = 0;
		boolean exitStatus = false;
		String sideInputPath = args[1];
		String inputPath = args[0];
		String outputPath = args[2];

		if (args.length < 2) {
			System.err.println("In sufficient arguments \n");
			System.exit(-1);
		}

		Job job = new Job(new Configuration());
		job.setJarByClass(PAYLAvgFTrainingTest.class);
		job.setJobName("AVGFTraining of PAYL");
		System.out.println(sideInputPath);
		addCacheFiles(sideInputPath, job);
		DistributedCache.createSymlink(job.getConfiguration());
		job.setInputFormatClass(PCAPFileInputFormat.class);

		PCAPFileInputFormat.addInputPath(job, new Path(inputPath));

		job.setOutputFormatClass(PAYLFileOutputFormat.class);
		PAYLFileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapperClass(PAYLAvgFTraining.PAYLMapper.class);
		job.setReducerClass(PAYLAvgFTraining.PAYLReducer.class);
		job.setMapOutputKeyClass(PAYLKey.class);
		job.setMapOutputValueClass(PAYLVWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PAYLAvgFWritable.class);
		job.setReduceSpeculativeExecution(true); 
		job.setNumReduceTasks(50);

		System.out.println("********** Starting the job now *********");
		startTime = new Date().getTime();
		exitStatus = job.waitForCompletion(true);
		finishTime = new Date().getTime();
		System.out.println("Time elapsed : "+ (finishTime - startTime));
		System.out.println("************** The END ****************");
		System.exit(exitStatus ? 0 : 1); 
	}
}
