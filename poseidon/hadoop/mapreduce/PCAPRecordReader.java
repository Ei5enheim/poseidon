/**
 * File name: PCAPRecordReader
 * Author: Rajesh Gopidi
 * Course: COMP 790-052
 * Final Project
 *
 * This file contains the definition of the Record
 * Reader for the PCAP file formats.
 */

package poseidon.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import poseidon.hadoop.mapreduce.*;
import poseidon.hadoop.io.*;
import poseidon.packet.*;
import java.io.*;
import java.util.*;
import java.lang.reflect.*;

public class PCAPRecordReader extends RecordReader <IntWritable, PacketWritable>
{
    private FileSplit fileSplit; 
    private Configuration conf;
    private Path file;
    private PacketWritable pw;
    private int pktCount = 0;
    // we split the files in mapreduce blocks
    private long fileSize = 0;
    private IntWritable key;
    private FileSystem fs;
    private PacketReader packetReader;
    private FSDataInputStream in = null;
    
    public PCAPRecordReader()
    {

    }
    public void close()
    {
    }

    public IntWritable getCurrentKey()
    {
        key.set(pktCount);
        return (key);
    }

    public PacketWritable getCurrentValue()
    {
        pw.packet = packetReader.nextPacket();
        return(pw);
    }

    public float getProgress()
    {
        return(packetReader.getProgress()/(float)fileSize);
    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
                                                                         InterruptedException
    {
        this.fileSplit = (FileSplit) split;
        fileSize = this.fileSplit.getLength();
        System.out.println("file split size: " + fileSize);
        this.conf = context.getConfiguration();
        file = fileSplit.getPath();
        fs = FileSystem.get(file.toUri(), conf);
        try {
            in = fs.open(file);
        } catch (IOException io) {
            System.err.println("Caught IO Exception");
        }
        packetReader = new PacketReader(in);
        key = new IntWritable(0);
        pw  = new PacketWritable();
    }

    public boolean nextKeyValue()
    {
        if (packetReader.hasOneMore()) {
            pktCount++;
            return (true);
        }
        return (false);
    } 
}
