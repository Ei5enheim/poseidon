/*
 * Author: Gopidi Rajesh                                         
 * File Name:  PAYLTestOPWritable.java                                   
 * Course: COMP790-042                                           
 * Final Project                                                
 *                                                               
 */    

package poseidon.hadoop.io;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Writable;

public class PAYLTestOPWritable implements Writable 
{
	
    public String time;
    public String date;
    public String dstIP;
    public String srcIP;
    public int dstPort;
    public int srcPort;
    public double dist;

    public PAYLTestOPWritable()
    {
    }

    public PAYLTestOPWritable (int dummy)
    {
	time = null;
	date = null;
    }

    public PAYLTestOPWritable (String date, String time, String dstIP, 
				String srcIP, int dstPort, int srcPort)
    {
	this.time = time;
	this.date = date;
	this.srcIP = srcIP;
	this.dstIP = dstIP;
	this.dstPort = dstPort;
	this.srcPort = srcPort;
    }

    public void set (String date, String time, String dstIP,
			String srcIP, int dstPort, int srcPort) 
    {                               
	this.time = time;
	this.date = date;
	this.dstIP = dstIP;
	this.dstPort = dstPort;
	this.srcPort = srcPort;
	this.srcIP = srcIP;
    }       


    public void write (DataOutput out) throws IOException
    {
	out.writeUTF(date);
	out.writeUTF(srcIP);
	out.writeUTF(dstIP);
	out.writeInt(srcPort);
	out.writeInt(dstPort);
	out.writeUTF(time);
    }

    public void readFields (DataInput in) throws IOException
    {
        date = in.readUTF();
	srcIP = in.readUTF();
	dstIP = in.readUTF();
	srcPort = in.readInt();
	dstPort = in.readInt();
	time = in.readUTF();
    }

    public boolean equals (Object ob)
    {
	PAYLTestOPWritable payl = null;

	if (ob instanceof PAYLTestOPWritable) {
	    payl = (PAYLTestOPWritable) ob;

	    if (payl.dstPort == dstPort) {
		if (dstIP.equals(payl.dstIP)) 
		    return (true);
	    }

	}
	return (false);
    }

    public String toString()
    {
	StringBuilder str = new StringBuilder();
	str.append(date);
	str.append('\t');
	str.append(srcIP);
	str.append('\t');
	str.append(dstIP);
	str.append('\t');
	str.append(srcPort);
	str.append('\t');
	str.append(dstPort);
	str.append('\t');
	str.append(time);
	str.append('\t');
	str.append(dist);
	return (str.toString());
    }

}
