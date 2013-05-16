/*
 * Author: Rajesh Gopidi
 * File Name: vim  PAYLTesting.java
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

public class PAYLTesting
{
    public static class PAYLMapper extends Mapper<IntWritable, PacketWritable,
						  PAYLKey, PAYLTestVWritable> 
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
	PAYLTestVWritable wr;
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
	    wr = new PAYLTestVWritable(0);
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
	    wr.srcPort = value.packet.sourcePort;
	    key.x = 0;
	    key.y = 0;
	    key.IP = value.packet.dstIP;
	    wr.srcIP = value.packet.sourceIP;
	    wr.date = value.packet.date;
	    wr.time = value.packet.time;

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

    public static class PAYLReducer extends Reducer <PAYLKey, PAYLTestVWritable,
						     NullWritable, PAYLTestOPWritable>
    {
	private MultipleOutputs <NullWritable, 
				 PAYLTestOPWritable> multipleOutputs;
	private double avgByteFreq[];
	private int[] threshold;
	private int i = 0;
	private int FREQ_ARRAY_SIZE = 256;
	private double[] SD;
	private PAYLTestOPWritable wr;
	private Path[] files = null;
	private String fileName;
	private Configuration config;
	private double sfactor = 0;
	private double distance = 0;
	HashMap <String, HashMap> table = null;
	ArrayList<String> list = null;
	HashMap <String, ArrayList<String>> srcIPMap = null;
	StringBuilder str;
	int[] tym;
	int[] deadline;
	int[] pktTime;	

	protected void setup(Context context) throws IOException,
						InterruptedException
	{
	    config = context.getConfiguration();
	    multipleOutputs = new MultipleOutputs <NullWritable, 
						PAYLTestOPWritable> (context);
	    SD  = new double [FREQ_ARRAY_SIZE];
	    avgByteFreq = new double [FREQ_ARRAY_SIZE];
	    wr = new PAYLTestOPWritable(0);
	    threshold = new int[4];
	    threshold[0] = context.getConfiguration().getInt("21", threshold[0]);
	    threshold[1] = context.getConfiguration().getInt("23", threshold[1]);   
	    threshold[2] = context.getConfiguration().getInt("25", threshold[2]); 
	    threshold[3] = context.getConfiguration().getInt("80", threshold[3]);
	    sfactor = context.getConfiguration().getInt("sfactor", 0)/(double)1000;	    
	    tym = new int[3];
	    deadline = new int[3];
	    pktTime = new int[3];
	    str = new StringBuilder();
 
            try {
                files = DistributedCache.getLocalCacheFiles(config);
		createAttackDataBase();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (files == null) {
                throw new IOException("Unable to load vector weigths");
            }
	}
	
	public int getPortThreshold (int port)
	{
		switch (port) {
			case (21):
				return (0);
			case (23):
				return (1);
			case (25):
				return (2);
			case (80):
				return (3);
			default:
				System.err.println("in default case");
		}
		return (0);
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

                for (i = 0; i < FREQ_ARRAY_SIZE; i++) {
                     SD[i] = in.readDouble();
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
	public void reduce (PAYLKey inputKey, Iterable<PAYLTestVWritable> values, Context output)
			    throws IOException, InterruptedException
	{
	    boolean cond = false;
	    String falseNPath = "falseNeg/"+ Integer.toString(inputKey.port)+ "/falseNegative";
	    String falsePPath = "falsePos/"+ Integer.toString(inputKey.port)+ "/falsePositive";	
	    String anomalies = "anomalies/"+ Integer.toString(inputKey.port)+ "/anomaly";
	    String normal = "normal/"+ Integer.toString(inputKey.port)+ "/normal";

	    if (readFile(inputKey)) {
		for (PAYLTestVWritable value: values) {
			list = null;
			wr.set (value.date, value.time, getIP(inputKey.IP),
				getIP(value.srcIP), inputKey.port, value.srcPort);
			cond = isItTrueHit(value.srcIP, inputKey.IP, value.srcPort,
					    inputKey.port, value.date, value.time);
			distance = calcDist(value);
			wr.dist = distance;
			distance -= (double) threshold[getPortThreshold(inputKey.port)];
			if (distance > 0) {
				if (cond) {
				    multipleOutputs.write(NullWritable.get(),
							    wr,
							    anomalies);
				} else {
                                    multipleOutputs.write(NullWritable.get(),
                                                            wr,
                                                            falsePPath);
				}

			} else {
				if (cond)
				    multipleOutputs.write(NullWritable.get(),
							    wr,
							    falseNPath);
				else 
                                    multipleOutputs.write(NullWritable.get(),
                                                            wr,
                                                            normal);
			}
		}
	    } else {
		// for now write it as a false negative.
	    }
	}

	protected void cleanup(Context context) throws IOException, 
		  					InterruptedException 
	{
		multipleOutputs.close();
	}

	public boolean matchdstIP (String testIP, String time,
				    int srcPort, int dstPort) 
	{
	    boolean ret = false, foundIP = false;
	    String value, dstIP, temp;

	    for (int i = 0; i < list.size(); i++) {
		foundIP = false;
		value = list.get(i);
		dstIP = value.substring(0, value.indexOf('@'));
		//System.out.println("\nAn entry in the list -->"+dstIP);
		if (dstIP.indexOf(testIP) != -1) {
		    foundIP = true;
		} else if (dstIP.indexOf('*') != -1) {
		    //System.out.println("An * entry in the list -->"+dstIP);
		    temp = testIP.substring(0, testIP.lastIndexOf(".")+1) + "*";
		    if (dstIP.indexOf(temp) != -1)
			foundIP = true;
		}

		if (!foundIP)
		    continue;		    
		/*source port
		temp = value.substring(value.indexOf('@')+1, value.indexOf("@@")); 
		if (!(temp.equals("-1")) && 
		    (temp.indexOf(Integer.toString(srcPort)) == -1))
		    continue;
		*/
		//destination port
		temp = value.substring(value.indexOf("@@")+2, value.indexOf("@@@")); 
		if (!(temp.equals("-1")) && 
		    (temp.indexOf(Integer.toString(dstPort)) == -1))
		    continue;

		if (checkForTimeStamp(time, value))
		    return (true);
	    }

	    return (false);
	}

	public boolean checkForTimeStamp(String time, String value)
	{
	    String actualTime = value.substring(value.indexOf("@@@")+3, value.indexOf('#'));
	    String duration = value.substring(value.indexOf('#')+1, value.length());

	    //System.out.println("actualTime " + actualTime + " duration "+ duration); 

	    int offset = 0;
	    for (int i = 0; i < 2; i++)
	    {
		tym[i] = Integer.parseInt(actualTime.substring(offset,
			    			actualTime.indexOf(':',offset)));
		deadline[i] = Integer.parseInt(duration.substring(offset, 
			    			duration.indexOf(':',offset)));
		pktTime[i] = Integer.parseInt(time.substring(offset, 
			    			time.indexOf(':',offset)));
		offset += 3;
		//System.out.println("tym["+i+"]=" + tym[i]+" deadline["+i+"]= "+
		//deadline[i]+" pktTime["+i+"]= "+ pktTime[i]);
	    }

	    tym[2] = Integer.parseInt(actualTime.substring(offset, actualTime.length()));
	    deadline[2] = Integer.parseInt(duration.substring(offset, duration.length()));
	    pktTime[2] = Integer.parseInt(time.substring(offset, time.indexOf(':',offset)));

	    //System.out.println("tym[2]=" + tym[2]+" deadline[2]= "+deadline[2]+
	    //			 " pktTime[2]= "+ pktTime[2]);

	    if (tym[2] + deadline[2] > 60) {
		deadline[1] += 1;
	    }

	    if (tym[1] + deadline[1] > 60) {
		deadline[0] += 1;
	    }
	   
	    if ((pktTime[0] >= tym[0]) && (pktTime[0] <= tym[0] + deadline[0])) {
		if ((pktTime[0] == tym[0] + deadline[0]) && (tym[0] + deadline[0] != tym[0])) {
			if (tym[1] + deadline[1] < 60){
				if (pktTime[1] <= tym[1] + deadline[1]) {
					if (pktTime[1] == tym[1] + deadline[1]) {
						if (tym[2] + deadline[2] <= 60)
							return (pktTime[2] <= (tym[2] + deadline[2] + 2));
						else 
							return (pktTime[2] <= (tym[2] + deadline[2] -60+2));
					}
					return (true);
				} 
			} else {
                                if (pktTime[1] <= (tym[1] + deadline[1] - 60)) {
					if (pktTime[1] == (tym[1] + deadline[1]-60)) {
						if (tym[2] + deadline[2] <= 60)
							return (pktTime[2] <= (tym[2] + deadline[2] + 2));
						else
							return (pktTime[2] <= (tym[2] + deadline[2] -60+2));
					}
                                        return (true);
                                }
			}
		} else if ((pktTime[0] == tym[0] + deadline[0])) {
			if ((pktTime[1] >= tym[1]) && (pktTime[1] <= tym[1] + deadline[1])) {
				if ((pktTime[1] == tym[1] + deadline[1]) && (tym[1] + deadline[1] != tym[1])) {
					if (tym[2] + deadline[2] < 60) {
						return (pktTime[2] <= tym[2] + deadline[2] + 2);
					} else {
						return (pktTime[2] <= (tym[2] + deadline[2] -60+2));
					}
				} else if (pktTime[1] == tym[1] + deadline[1]) {
					return ((pktTime[2] >= tym[2]) &&
						(pktTime[2] <= tym[2] + deadline[2] + 2));
				} else { 
					return (true);
				}
			}
		} else {
			return (true);
		}
	    }
	    return (false);    
	}

	public boolean isItTrueHit (int[] srcIP, int[] dstIP, int srcPort,
				    int dstPort, String date, String time)
	{
	    if (table.containsKey(date)) {
		srcIPMap = table.get(date);
		String src = getIP(srcIP);
		if (srcIPMap.containsKey(src))
		{
		    list = srcIPMap.get(src);

		} else if (srcIPMap.containsKey("xxx.xxx.xxx.xxx")){
		    list = srcIPMap.get("xxx.xxx.xxx.xxx");                           
		}

		if (list == null)
		    return (false);

		String dst = getIP(dstIP);
		
		return(matchdstIP(dst, time, srcPort, dstPort));
	    }
	    return (false);
	}

	public double calcDist (PAYLTestVWritable value)
	{
	    double dist = 0;
	    double temp = 0;
	    for (i = 0; i < FREQ_ARRAY_SIZE; i++) {
		temp = (((double)value.freq[i]/value.payloadSize) -
					avgByteFreq[i])/(SD[i]+sfactor);
		if (temp < 0)
		    dist += (-temp);
		else
		    dist += temp;
	    }
	    return ((dist/*-threshold*/));
	}

	public boolean createAttackDataBase() throws IOException
	{
	    Path attack_db = null;
	    FileInputStream fin = null;
	    String line;
	    String[] tokens;
	    String value;

	    for (Path file: files) {
		if (file.toString().matches(".*attack_database")){
		    attack_db = file;
		    break;
		}
	    }

	    if (attack_db == null)
		return (false);

	    fin = new FileInputStream(new File(attack_db.toString()));
	    BufferedReader in = new BufferedReader(new InputStreamReader(fin));
	    table = new HashMap <String, HashMap> (35);

	    if ((fin == null) || (in == null))
		return (false);

	    while ((line = in.readLine()) != null) {

		tokens = line.split("\\s");
		value = tokens[4] + '@'+ tokens[5] + "@@" + tokens[6] + "@@@" + tokens[1] + '#' + tokens[2];

		if (table.containsKey(tokens[0]))
		{
		    srcIPMap = table.get(tokens[0]);
		    if (srcIPMap.containsKey(tokens[3])) {
			list = srcIPMap.get(tokens[3]);
		    } else {
			list = new ArrayList<String>();
		    }
		    list.add(value);
		    srcIPMap.put(tokens[3], list);
		} else {
		    srcIPMap = new HashMap<String, ArrayList<String>> (20);
		    list = new ArrayList<String>();
		    list.add(value);
		    srcIPMap.put(tokens[3], list);
		    table.put(tokens[0], srcIPMap);               
		}
	    }
	    fin.close();
	    in.close();
	    return (true);	
	}

	public String getIP (int[] array)
	{
	    String string;
	    str.append(array[3]);
	    str.append('.');
	    str.append(array[2]);
	    str.append('.');
	    str.append(array[1]);
	    str.append('.');
	    str.append(array[0]);
	    string = str.toString();
	    str.delete(0, str.length());
	    return (string);
	}

    }
}
