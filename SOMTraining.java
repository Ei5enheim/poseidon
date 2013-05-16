/*
 * Author: Gopidi Rajesh
 * File Name: SOMTraining.java
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

public class SOMTraining
{
    public static class SOMMapper extends Mapper<IntWritable, PacketWritable,
	   GridKey, BatchSOMUpdateWritable> 
	   {
	       int[] wNeuron = new int[2];	
	       double nbrHoodWidth = 0;
	       int epochNumber = 0;
	       int sigma = 0;
	       int timeConst = 0;
	       int xDim = 0;
	       int yDim = 0;
	       double [][][][] weigthVectors;
	       int[] portNumbers;
	       boolean [] doneReading;
	       double[] vector;
	       double[] backup_vector;
	       boolean[] LRU;
	       GridKey key;
	       Configuration config;
	       BatchSOMUpdateWritable wr;
	       double MAX_PAYLOAD_CMP_VAL = 256;
	       int NO_OF_PORTS = 1025;
	       int PAYLOAD_SIZE = 1460;
	       int WARRAY_SIZE = 10;
	       int index = 0, i = 0;
	       int lastReplacement = -1;
	       StringBuilder str;
	       String fileName;
	       int counter = 0;
		Path[] files = null;

	       protected void setup(Context context) throws IOException, InterruptedException
	       {
		   config = context.getConfiguration();
		   // check before removing
		   sigma = context.getConfiguration().getInt("sigma", sigma);
		   epochNumber = context.getConfiguration().getInt("IterationNumber", epochNumber);
		   timeConst = context.getConfiguration().getInt("TimeConstant", timeConst);
		   xDim = context.getConfiguration().getInt("X Dimension", xDim);
		   yDim = context.getConfiguration().getInt("Y Dimension", yDim);
		   nbrHoodWidth = calcNbrHoodWidth();
		   doneReading = new boolean [NO_OF_PORTS];
		   weigthVectors = new double [WARRAY_SIZE][xDim][yDim][PAYLOAD_SIZE];
		   portNumbers = new int [WARRAY_SIZE];
		   initializePortNumbers();
		   vector = new double[PAYLOAD_SIZE];
		   backup_vector = new double[PAYLOAD_SIZE];
		   key = new GridKey();
		   wr = new BatchSOMUpdateWritable((double) 0);
		   LRU = new boolean[WARRAY_SIZE];
		   str = new StringBuilder();
                   try {
		       /*Path path = new Path("/user/rajesh/output/iteration"+(epochNumber-1));
		       FileSystem fs = FileSystem.get(path.toUri(), config);
		       FileStatus[] fstatus = fs.listStatus(path);
			if (fstatus.length < 1)
				files = null;
			files = new Path[fstatus.length];
			getFiles(files, fstatus);
			*/
                       files = DistributedCache.getLocalCacheFiles(config);
                   } catch (IOException e) {
                       e.printStackTrace();
                   }
                   if (files == null) {
                       throw new IOException("Unable to load vector weigths");
                   }
	       }

		public void getFiles (Path[] files, FileStatus[] fstatus)
		{
			int i = 0;
			for (FileStatus status: fstatus) {
				files[i] = status.getPath();
				i++;
			}	
		} 

	       public void initializePortNumbers()
	       {
		   for (int i = 0; i < WARRAY_SIZE; i++)
		       portNumbers[i] = -1;

	       }
	    
               
	       public void readFile (Path file, double[] vector) throws IOException
	       {
		   //FileSystem fs;
		   //FSDataInputStream in = null;
		   FileInputStream fs = null;
		   DataInputStream in = null;
		   try {
		        //fs = FileSystem.get(file.toUri(), config);
			//in = fs.open(file);
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
		   str.append("iteration");
		   str.append(epochNumber-1);
		   str.append('/');
		   if (epochNumber != 1)
		       str.append(port);
		   else 
		       str.append(1);
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

		/*
		   try {
		       files = DistributedCache.getLocalCacheFiles(config);
		   } catch (IOException e) {
		       e.printStackTrace();
		   }
		   if (files == null) {
		       throw new IOException("Unable to load vector weigths");
		   }
		 */

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

	       public double calcNbrHoodWidth ()
	       {
		   return (sigma * Math.pow (Math.E, -(double)(epochNumber/timeConst)));
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
		   int temp = 0;
		   key.port = value.packet.dstPort;
		   key.x = 0;
		   key.y = 0;
		   for (i = 0; i < value.packet.payloadSize; i++) {
		       if (value.packet.payload[i] < 0)
			   vector[i] = convertToInt(value.packet.payload[i])/MAX_PAYLOAD_CMP_VAL;
		       else 
			   vector[i] = value.packet.payload[i]/MAX_PAYLOAD_CMP_VAL;
		   }

		   for (i = value.packet.payloadSize; i < PAYLOAD_SIZE; i++) {
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

		   for (int x = 0; x < xDim; x++) {
		       for (int y = 0; y < yDim; y++) {
			   wr.den = calcNbrHoodFunction(wNeuron[0], x+1, 
				   wNeuron[1], y+1);
			   key.x = x + 1;
			   key.y = y + 1;
			   for (i = 0; i < PAYLOAD_SIZE; i++)
			   {
			       backup_vector[i] = vector[i] * wr.den;
			   }
			   wr.num = backup_vector;
			   output.write (key, wr);
		       }
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

	       public double calcNbrHoodFunction(int x1, int y1, int x2, int y2)
	       {
		   double dist = Math.pow((x1 - x2), 2) + Math.pow((y1 - y2), 2);

		   return (Math.pow(Math.E, -(dist/Math.pow(nbrHoodWidth, 2))));

	       }

	   } 

    public static class SOMCombiner extends Reducer <GridKey, BatchSOMUpdateWritable, GridKey, BatchSOMUpdateWritable>
    {
	private double[] num;
	private double den = 0; 
	private int i = 0;
	private BatchSOMUpdateWritable wr;
	protected void setup(Context context)
	    throws IOException,
		   InterruptedException
		   {
		       num = new double[NeuronVectorWritable.MAX_PAYLOAD_SIZE];
		       wr  = new BatchSOMUpdateWritable((double) 0);
		   }

	//@Override
	public void reduce (GridKey inputKey, Iterable<BatchSOMUpdateWritable> values, Context output)
	    throws IOException, InterruptedException
	    {
		den = 0;
		for (BatchSOMUpdateWritable value: values) {
		    den += value.den;
		    for (i = 0; i < value.num.length; i++) {
			num[i] += value.num[i];	
		    }
		}
		wr.num = num;
		wr.den = den;
		output.write(inputKey, wr);
		
		for (i = 0; i < num.length; i++) {
		    num[i] = 0;
		}

	    }
    }

    public static class SOMReducer extends Reducer <GridKey, BatchSOMUpdateWritable, NullWritable, NeuronVectorWritable>
    {
	private MultipleOutputs <NullWritable, 
		NeuronVectorWritable> multipleOutputs;
	private double[] num;
	private double den = 0; 
	private int i = 0;
	public NeuronVectorWritable wr;

	protected void setup(Context context)
	    throws IOException,
		   InterruptedException
		   {
		       num = new double[NeuronVectorWritable.MAX_PAYLOAD_SIZE];
		       multipleOutputs = new MultipleOutputs <NullWritable, 
				       NeuronVectorWritable> (context);
			wr = new NeuronVectorWritable(0);
		   }

	//@Override
	public void reduce (GridKey inputKey, Iterable<BatchSOMUpdateWritable> values, Context output)
	    throws IOException, InterruptedException
	    {
		den = 0;
		StringBuilder str = new StringBuilder();
		str.append(inputKey.port);
		str.append('-');
		str.append(inputKey.x);
		str.append('-');
		str.append(inputKey.y);
		String basePath = str.toString();
		for (BatchSOMUpdateWritable value: values) {
		    den += value.den;
		    for (i = 0; i < value.num.length; i++) {
			num[i] += value.num[i];	
		    }
		}

		for (i = 0; i < num.length; i++) {
		    num[i] = num[i]/den;
		}
		wr.set(num);
		multipleOutputs.write(NullWritable.get(), 
			wr/*new NeuronVectorWritable(m)*/,
			basePath);
		for (i = 0; i < num.length; i++) {
		    num[i] = 0;
		}
	    }

	protected void cleanup(Context context) throws IOException, 
		  InterruptedException 
		  {
		      multipleOutputs.close();
		  }		
    }
}


/*
               public Path findFile(FileStatus[] files, int port, int x, int y)
               {
                   str.append('.');
                   str.append('*');
                   str.append("iteration");
                   str.append(epochNumber-1);
                   str.append('/');
                   if (epochNumber != 1)
                       str.append(port);
                   else
                       str.append(1);
                   str.append('-');
                   str.append(x);
                   str.append('-');
                   str.append(y);
                   str.append('.');
                   str.append('*');
                   fileName = str.toString();
                   str.delete(0, str.length());

                   for (FileStatus file: files) {
                       if (file.getPath()toString().matches(fileName))
                           return (file.getPath());
                   }
                   return (null);
               }

*/
