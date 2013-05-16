package poseidon.packet;

import java.io.*;
import java.util.*;

import poseidon.packet.Packet;
import poseidon.packet.PacketReader;
import java.io.*;
import java.util.*;

public class PacketReaderTest
{
    public static DataInputStream is;
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
    double MAX_PAYLOAD_CMP_VAL = 256;
    int NO_OF_PORTS = 1025;
    int PAYLOAD_SIZE = 1460;
    int WARRAY_SIZE = 10;
    int index = 0, i = 0;
    int lastReplacement = -1;
    StringBuilder str;
    String fileName;
    int counter = 0;
    double den = 0;

    protected void setup() throws IOException, InterruptedException
    {
	    sigma = 4;
	    timeConst = 100;
	    xDim = 12;
	    yDim = 8;
	    nbrHoodWidth = calcNbrHoodWidth();
	    doneReading = new boolean [NO_OF_PORTS];
	    weigthVectors = new double [WARRAY_SIZE][xDim][yDim][PAYLOAD_SIZE];
	    portNumbers = new int [WARRAY_SIZE];
	    initializePortNumbers();
	    vector = new double[PAYLOAD_SIZE];
	    backup_vector = new double[PAYLOAD_SIZE];
	    LRU = new boolean[WARRAY_SIZE];
	    str = new StringBuilder();
    }

    public void initializePortNumbers()
    {
	    for (int i = 0; i < WARRAY_SIZE; i++)
		    portNumbers[i] = -1;

    }

    public void readFile (File file, double[] vector) throws IOException
    {
	    FileInputStream fs = null;
	    DataInputStream in = null;
	    try {
		    fs = new FileInputStream(file);
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

    public File findFile(int port, int x, int y)
    {
	str.append("iteration0/");
	str.append(1);
	str.append('-');
	str.append(x);
	str.append('-');
	str.append(y);
	fileName = str.toString();
	str.delete(0, str.length());

	return (new File(fileName));
    }

    public boolean readVWeigths (int index) throws IOException
    {

	File file = null;
	int port = portNumbers[index];

	for (int x = 0; x < xDim; x++) {
	    for (int y = 0; y < yDim; y++) {
		try {
		    if ((file = findFile (port, x+1, y+1)) == null) {
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
		System.out.println("temp= " + temp + "mindst= "+ minDst);
		if (temp < minDst) {
		    minDst = temp;
		    wNeuron[0] = x+1;
		    wNeuron[1] = y+1;
		    System.out.println("port number= "+ portNumbers[index]+ "index = " + index + "x = " + x +" y = " + y);
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

    public void map (Packet packet) throws IOException, InterruptedException
    {
	int temp = 0;
	for (i = 0; i < packet.payloadSize; i++) {
	    if (packet.payload[i] < 0)
		vector[i] = convertToInt(packet.payload[i])/MAX_PAYLOAD_CMP_VAL;
	    else 
		vector[i] = packet.payload[i]/MAX_PAYLOAD_CMP_VAL;
	}

	for (i = packet.payloadSize; i < PAYLOAD_SIZE; i++) {
	    vector[i] = 0;
	}
	wNeuron[0] = 0;
	wNeuron[1] = 0;
	try {
	    index = findWVectorIndex(packet.dstPort);
	    findWinningNeuron(vector, index); 

	} catch (IOException io) {
	    throw new IOException("Unable to find the winning neuron");
	}

	for (int x = 0; x < xDim; x++) {
	    for (int y = 0; y < yDim; y++) {
		den = calcNbrHoodFunction(wNeuron[0], x+1, 
			wNeuron[1], y+1);
		for (i = 0; i < PAYLOAD_SIZE; i++)
		{
		    backup_vector[i] = vector[i] * den;
		    if (backup_vector[i] != 0)
			temp++;
		}
	    }
	}
	if (counter < 5) {
	    System.err.println("\n*** "+ wNeuron[0] + "***"+wNeuron[1] +" ****+ "+den +"\n");
	    counter++;
	}
	throw new IOException("Unable to find the winning neuron");
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


    public static void main(String[] args) throws Exception
    {
	is = new DataInputStream(new FileInputStream(new File("attakfree/foutside_traffic_1_6")));
	Packet packet;
	PacketReaderTest test = new PacketReaderTest();	
	PacketReader reader = new PacketReader(is);
	while (reader.hasOneMore())
	{
	    packet = reader.nextPacket();
	    System.out.println(packet.dstIP[3] +"."+packet.dstIP[2] +"."+packet.dstIP[1] +"."+packet.dstIP[0]);
	    test.map (packet);
	}
	is.close();
    }
}

