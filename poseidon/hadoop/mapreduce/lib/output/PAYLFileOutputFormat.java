/**
 * File name: PAYLFileOutputFormat.java
 * Author: Rajesh Gopidi
 * Course: COMP 790-052
 * Final Project
 *
 * This file contains the definitions of the classes
 * that are used to write the neuron weight vectors to
 * output directory in binary format.
 */

package poseidon.hadoop.mapreduce.lib.output;

import poseidon.hadoop.mapreduce.*;
import poseidon.hadoop.io.*;

import java.util.*;
import java.io.*;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class PAYLFileOutputFormat extends FileOutputFormat <NullWritable, PAYLOPWritable>
{
	FileSystem fs;
	Path file;

	public PAYLFileOutputFormat()
	{

	}

	public RecordWriter <NullWritable, PAYLOPWritable> getRecordWriter(TaskAttemptContext job) 
									throws IOException
	{
		file = getDefaultWorkFile(job, "");
		fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut = fs.create(file,
						       false);	
		return (new ByteWriter <NullWritable, PAYLOPWritable> (fileOut));
	}

}
