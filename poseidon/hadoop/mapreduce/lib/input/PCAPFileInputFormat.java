/**
 * File name: PCAPFileInputFormat
 * Author: Rajesh Gopidi
 * Course: COMP 790-052
 * Final Project
 *
 * This file contains the definitions of the classes
 * that are used to read the data from pcap files on
 * record basis.
 */

package poseidon.hadoop.mapreduce.lib.input;

import poseidon.hadoop.mapreduce.*;
import poseidon.hadoop.io.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class PCAPFileInputFormat extends FileInputFormat<IntWritable, PacketWritable> 
{
    public RecordReader<IntWritable, PacketWritable> createRecordReader(InputSplit split,
                                                    TaskAttemptContext context) 
                                                    throws IOException,
                                                        InterruptedException
    {

        PCAPRecordReader reader = new PCAPRecordReader();
        reader.initialize(split, context);
        return (reader); 
    }
}


