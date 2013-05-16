/*
 * Author: Rajesh Gopidi
 * File Name: vim  PAYLTraining.java
 * Course: COMP790-042
 * Assignment: Final Project
 *
 */

import java.io.*;
import java.util.*;
import java.net.*;
import java.lang.Math;

import poseidon.hadoop.io.*;
import poseidon.packet.*;
import poseidon.hadoop.mapreduce.*;
import poseidon.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class PAYLSDTraining
{
    public static class PAYLMapper extends Mapper<IntWritable, PacketWritable,
						  PAYLKey, PAYLVWritable> 
    {
	int[] wNeuron = new int[2];	
	short[] freq;
	double [][][][] weigthVectors;
	int[] portNumbers;
	boolean [] doneReading;
	double[] vector;
	int[] payload;
	boolean[] LRU;
	PAYLKey key;
	Configuration config;
	PAYLVWritable wr;
	StringBuilder str;
        String fileName;
        Path[] files = null;
        int xDim = 12;
        int yDim = 8;
	double MAX_PAYLOAD_CMP_VAL = 256;
	int NO_OF_PORTS = 1025;
	int PAYLOAD_SIZE = 1460;
	int FREQ_ARRAY_SIZE = 256;
	int WARRAY_SIZE = 10;
	int index = 0, i = 0;
	int lastReplacement = -1;

	protected void setup(Context context) throws IOException, InterruptedException
	{
	    config = context.getConfiguration();
	    // check before removing
	    doneReading = new boolean [NO_OF_PORTS];
	    weigthVectors = new double [WARRAY_SIZE][xDim][yDim][PAYLOAD_SIZE];
	    portNumbers = new int [WARRAY_SIZE];
	    initializePortNumbers();
	    vector = new double[PAYLOAD_SIZE];
	    payload = new int[PAYLOAD_SIZE];
	    key = new PAYLKey(0, 0);
	    wr = new PAYLVWritable(0);
	    LRU = new boolean[WARRAY_SIZE];
	    str = new StringBuilder();
	    freq = new short[FREQ_ARRAY_SIZE];			

	    try {
		files = DistributedCache.getLocalCacheFiles(config);
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	    if (files == null) {
		throw new IOException("Unable to load vector weigths");
	    }
	}

	public void initializePortNumbers()
	{
	    for (int i = 0; i < WARRAY_SIZE; i++)
		portNumbers[i] = -1;
	}

	public void readFile (Path file, double[] vector) throws IOException
	{
	    FileInputStream fs = null;
	    DataInputStream in = null;
	    try {
		fs = new FileInputStream(file.toString());
		in = new DataInputStream(fs);
		for (int i = 0; i < PAYLOAD_SIZE; i++) {	
		    vector[i] = in.readDouble();
		}
		in.close();
		fs.close();
	    } catch (IOException io) {
		System.err.println("Caught IO Exception in read file");
		io.printStackTrace();
		throw new IOException ("unable to open file" + file.toString());
	    }
	}

	public Path findFile(Path[] files, int port, int x, int y)
	{
	    str.append('.');
	    str.append('*');
	    str.append(port);
	    str.append('-');
	    str.append(x);
	    str.append('-');
	    str.append(y);
	    str.append('.');
	    str.append('*');
	    fileName = str.toString();
	    str.delete(0, str.length());

	    for (Path file: files) {
		if (file.toString().matches(fileName)) 
		    return (file);
	    }
	    return (null);
	}

	public boolean readVWeigths (int index) throws IOException
	{
	    Path file = null;
	    int port = portNumbers[index];

	    for (int x = 0; x < xDim; x++) {
		for (int y = 0; y < yDim; y++) {
		    try {
			if ((file = findFile (files, port, x+1, y+1)) == null) {
			    throw new IOException("Unable to load vector weigths");
			}
			readFile (file, weigthVectors[index][x][y]);		
		    } catch (IOException io) {
			throw new IOException("Unable to load vector weigths");
		    }
		}
	    }	

	    doneReading[portNumbers[index]] = true;
	    return (true);
	}

	public int convertToInt(byte value)
	{
	    int i = 0x0000007F;
	    i = i & value;
	    i = (0x00000080) | i; 
	    return (i);
	}

	public void findWinningNeuron (double[] vector, int index) throws IOException
	{
	    double minDst = 0x7FFFFFFF;
	    double temp = 0;

	    if (!doneReading[portNumbers[index]]) {
		if (!readVWeigths(index)) {
		    throw new IOException("ubable to read weight vectors");
		}
	    }

	    for (int x = 0; x < xDim; x++) {
		for (int y = 0; y < yDim; y++) {
		    temp = calcManhattanDist(vector,
			    weigthVectors[index][x][y]);
			//System.err.println("\n ****** minDst = "+ minDst + "temp= "+ temp + "x= "+ x +"y= "+y);
		    if (temp < minDst) {
			minDst = temp;
			// remember the offset is 1
			wNeuron[0] = x+1;
			wNeuron[1] = y+1;
		    }
		}
	    }
	}

	public void resetLRU()
	{
	    for (int i = 0; i < WARRAY_SIZE; i++) {
		LRU[i] =false;
	    }

	}
	public int findWVectorIndex (int port) 
	{
	    if (doneReading[port]) 
	    {
		for (int index = 0; index < WARRAY_SIZE; index++) {
		    if (portNumbers[index] == port) {
			LRU[index] = true;
			return (index);
		    }
		}

	    } else {
		for (int i = lastReplacement + 1; i < WARRAY_SIZE; 
			i++) {
		    if (LRU[i] == false) {
			lastReplacement = i;
			if (portNumbers[i] != -1)
			    doneReading[portNumbers[i]]= false;
			portNumbers[i] = port;
			resetLRU();
			LRU[i] = true;
			return (i);
		    }
		    LRU[i] = false;
		}
		for (int i = 0; i < lastReplacement; i++) {
		    if (LRU[i] == false) {
			lastReplacement = i;
			if (portNumbers[i] != -1)
			    doneReading[portNumbers[i]]= false;
			portNumbers[i] = port;
			resetLRU();
			LRU[i] = true;
			return (i);
		    }
		    LRU[i] = false;
		} 	
	    }
	    return (findWVectorIndex(port));
	}

	//@Override 
	public void map (IntWritable inputKey, PacketWritable value, 
			    Context output) throws IOException, InterruptedException
	{
	    key.port = value.packet.dstPort;
	    key.x = 0;
	    key.y = 0;
	    key.IP = value.packet.dstIP;

	    for (i = 0; i < value.packet.payloadSize; i++) {
		if (value.packet.payload[i] < 0) 
		    payload[i] = convertToInt(value.packet.payload[i]);
		else 
		    payload[i] = value.packet.payload[i];

		    vector[i] = payload[i]/MAX_PAYLOAD_CMP_VAL;
	    }

	    for (i = value.packet.payloadSize; i < PAYLOAD_SIZE; i++) {
		payload[i] = 0;
		vector[i] = 0;
	    }

	    wNeuron[0] = 0;
	    wNeuron[1] = 0;
	    try {
		index = findWVectorIndex(key.port);
		findWinningNeuron(vector, index); 
	    } catch (IOException io) {
		throw new IOException("Unable to find the winning neuron");
	    }

	    key.x = wNeuron[0];
	    key.y = wNeuron[1];

	    findByteFrequencies(value.packet.payloadSize);

	    wr.freq = freq;
	    wr.payloadSize = value.packet.payloadSize;
	    output.write (key, wr);
	   
	    for (i = 0; i < FREQ_ARRAY_SIZE; i++)
		freq[i] = 0;
	
	    //throw new IOException("this isn't working dude");

	}

	public void findByteFrequencies(int payloadSize)
	{

	    for (i = 0; i < payloadSize; i++) {
		freq[payload[i]]++;
	    }
	}

	public double calcManhattanDist (double[] inputv, double[] weigthv)
	{
	    double dist = 0;
	    double temp = 0;
	    for (int i = 0; i < PAYLOAD_SIZE; i++) {
		temp = inputv[i]-weigthv[i];
		if (temp < 0)
		    dist += (-temp);
		else
		    dist += temp;
	    }
	    return (dist);
	}
    }

    public static class PAYLReducer extends Reducer <PAYLKey, PAYLVWritable, NullWritable, PAYLOPWritable>
    {
	private MultipleOutputs <NullWritable, 
				 PAYLOPWritable> multipleOutputs;
	private Path[] files = null;
        private String fileName;
        private Configuration config;
	private double avgByteFreq[];
	private int i = 0;
	private int FREQ_ARRAY_SIZE = 256;
	private double[] SD;
	private PAYLOPWritable wr;
	private StringBuilder str;

	protected void setup(Context context) throws IOException,
						InterruptedException
	{
	    config = context.getConfiguration();
	    multipleOutputs = new MultipleOutputs <NullWritable, 
						PAYLOPWritable> (context);
	    SD  = new double [FREQ_ARRAY_SIZE];
	    avgByteFreq = new double [FREQ_ARRAY_SIZE];
	    wr = new PAYLOPWritable(0);
	    str = new StringBuilder();
            try {
                files = DistributedCache.getLocalCacheFiles(config);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (files == null) {
                throw new IOException("Unable to load vector weigths");
            }
	}

        public boolean readFile (PAYLKey key) throws IOException
        {
            FileInputStream fs = null;
            DataInputStream in = null;
            Path file = null;

            if ((file = findFile(key)) == null)
                return (false);

            try {
                fs = new FileInputStream(file.toString());
                in = new DataInputStream(fs);

                for (i = 0; i < FREQ_ARRAY_SIZE; i++) {
                    avgByteFreq[i] = in.readDouble();
                }
                in.close();
                fs.close();
            } catch (IOException io) {
                System.err.println("Caught IO Exception in read file");
                io.printStackTrace();
                throw new IOException ("unable to open file" + file.toString());
            }
            return (true);
	}

        public Path findFile(PAYLKey key)
        {
            str.append('.');
            str.append('*');
            str.append(key.IP[3]);
            str.append("\\.");
            str.append(key.IP[2]);
            str.append("\\.");
            str.append(key.IP[1]);
            str.append("\\.");
            str.append(key.IP[0]);
            str.append('-');
            str.append(key.x);
            str.append('-');
            str.append(key.y);
            str.append('-');
            str.append(key.port);
            str.append('.');
            str.append('*');
            fileName = str.toString();
            str.delete(0, str.length());

            for (Path file: files) {
                if (file.toString().matches(fileName))
                    return (file);
            }
            return (null);
        }
	
	//@Override
	public void reduce (PAYLKey inputKey, Iterable<PAYLVWritable> values, Context output)
			    throws IOException, InterruptedException
	{
	    double noOfPackets = 0;
	    String basePath = inputKey.toString();

	    if (readFile(inputKey)) {
		for (PAYLVWritable value: values) {
		    for (i = 0; i < FREQ_ARRAY_SIZE; i++) {
			SD[i] += Math.pow((avgByteFreq[i] -
					  ((double)value.freq[i]/value.payloadSize)), 2);
		    }
		    noOfPackets++;
		}

		for (i = 0; i < FREQ_ARRAY_SIZE; i++) {
		    System.err.println("SD["+i+"]= "+ SD[i]);
		    SD[i] = Math.sqrt(SD[i]/noOfPackets);
		}
		wr.set(avgByteFreq, SD);
		multipleOutputs.write(NullWritable.get(), 
					wr,
					basePath);
		for (i = 0; i < FREQ_ARRAY_SIZE; i++) {
		    SD[i] = 0;
		}
	    } else {
		throw new IOException("cannot find the file to read av byte frequencies");
	    }
	}

	protected void cleanup(Context context) throws IOException, 
		  InterruptedException 
	{
			  multipleOutputs.close();
	}		
    }
}

