/**         
 * File name: ByteWriter.java
 * Author: Rajesh Gopidi
 * Course: COMP 790-052
 * Final Project
 *      
 * This file contains the definition of the Record
 * writer that writes key values in binary format.
 */

package poseidon.hadoop.mapreduce;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.*;

public class ByteWriter <K extends Writable, V extends Writable> extends RecordWriter <K, V>
{
	DataOutputStream out;

	public ByteWriter ()
	{

	}

	public ByteWriter (DataOutputStream out)
	{
		this.out = out;
	}

	public void write (K key, V value) throws IOException
	{
		key.write(out);
		value.write(out);
	}

	public void close(TaskAttemptContext context) throws IOException
	{
		out.close();
	}
}
