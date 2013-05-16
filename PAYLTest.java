/*
 * Author: Gopidi Rajesh
 * File Name: PAYLTestingTest.java 
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

public class PAYLTest
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

	public static void addAttackFile(String fileName, Job job)
	{
		Path file = new Path(fileName);
		DistributedCache.addCacheFile(file.toUri(),
						job.getConfiguration());
	}

	public static void main (String args[]) throws Exception
	{
		long startTime = 0, finishTime = 0;
		boolean exitStatus = false;
		String SOMWVInput = args[0];
		String PAYLVInput =  args[1];
		String inputPath = args[2];
		String outputPath = args[3];
		String attackfile = "/user/rajesh/attack_database";

		if (args.length < 2) {
			System.err.println("In sufficient arguments \n");
			System.exit(-1);
		}

		//Configuration conf = new Configuration(); 
		Job job = new Job(new Configuration());
		job.setJarByClass(PAYLTest.class);
		job.setJobName("Testing of PAYL");
		addCacheFiles(SOMWVInput, job);
                addCacheFiles(PAYLVInput, job);
		addAttackFile(attackfile, job);

		DistributedCache.createSymlink(job.getConfiguration());
		job.setInputFormatClass(PCAPFileInputFormat.class);

		PCAPFileInputFormat.addInputPath(job, new Path(inputPath));
		// going with Text for output format
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapperClass(PAYLTesting.PAYLMapper.class);
		job.setReducerClass(PAYLTesting.PAYLReducer.class);
		job.setMapOutputKeyClass(PAYLKey.class);
		job.setMapOutputValueClass(PAYLTestVWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PAYLTestOPWritable.class);
		job.setReduceSpeculativeExecution(true); 
		job.getConfiguration().setInt("21", 75);
		job.getConfiguration().setInt("23", 100);
		job.getConfiguration().setInt("25", 125);
		job.getConfiguration().setInt("80", 90);
		job.getConfiguration().setInt("sfactor", 1);
		job.setNumReduceTasks(80);

		System.out.println("********** Starting the job now *********");
		startTime = new Date().getTime();
		exitStatus = job.waitForCompletion(true);
		finishTime = new Date().getTime();
		System.out.println("Time elapsed : "+ (finishTime - startTime));
		System.out.println("************** The END ****************");
		System.exit(exitStatus ? 0 : 1); 
	}
}
