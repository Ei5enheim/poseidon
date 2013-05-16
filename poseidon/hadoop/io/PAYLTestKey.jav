/*
 * Author: Gopidi Rajesh                                         
 * File Name: PAYLTestKey.java                                   
 * Course: COMP790-042                                           
 * Final Project                                                
 *                                                               
 */    

package poseidon.hadoop.io;

import java.io.*;
import java.util.*;
import java.lang.Math;
import org.apache.hadoop.io.*;

public class PAYLTestKey implements WritableComparable<PAYLTestKey>
{
	public int x,y;
	public int dstPort;
	public int sourcePort;
	public int[] dstIP;
	public int[] srcIP;
	public String date;
	public String time;
	
	public PAYLTestKey()
	{
		x = 0;
		y = 0;
		dstPort = 0;
		srcIP = new int[4];
		dstIP = new int[4];
		date = null;
		time = null;
	}

	public PAYLTestKey (int x, int y)
	{
		this.x = x;
		this.y = y;
		dstPort = 0;
		sourcePort = 0;
		dstIP = null;
		srcIP = null;
		date = null;
		time = null;
	}

	public PAYLTestKey (int x, int y, int dstPort, int[] dstIP,
			String date, String time)
	{
		this.x = x;
		this.y = y;
		this.dstPort = dstPort;
		this.dstIP = dstIP;
		this.date = date;
		this.time = time;
	}

	public int hashCode()
	{
		int i = 0;
		i = dstIP[0] + dstIP[1]*255 + dstIP[2] *255*255;
		//need to return to this
		return (i+(x-1)*8 + y +dstPort);
	}

	public int compareTo(PAYLTestKey key)
	{
		String ip1 = Arrays.toString(key.dstIP);
		String ip2 = Arrays.toString(dstIP);
		if (ip2.equals(ip1)) {
			if (dstPort > key.dstPort)
				return (1);
			else if (dstPort < key.dstPort)
				return (-1);
			if (x > key.x)
				return (1);
			else if (x < key.x) 
				return (-1);
			if (y > key.y)
				return (1);
			else if (y < key.y)
				return (-1);
			// when all the values are equal we return zero
			return (0);
		}
		return (ip2.compareTo(ip1));
	}

	public boolean equals (Object ob)
	{
		PAYLTestKey key = null;

		if (ob instanceof PAYLTestKey) {
			key = (PAYLTestKey) ob;
			
			if (this.compareTo(key) == 0)
				return (true);
		}
		return (false);
	}
	
	public void readFields (DataInput in) throws IOException
	{
		x = in.readInt();
		y = in.readInt();
		dstPort = in.readInt();
		sourcePort = in.readInt();
		dstIP[0] = in.readInt();
		dstIP[1] = in.readInt();
		dstIP[2] = in.readInt();
		dstIP[3] = in.readInt();
                srcIP[0] = in.readInt();
                srcIP[1] = in.readInt();
                srcIP[2] = in.readInt();
                srcIP[3] = in.readInt();
		date =  in.readUTF();
		time =  in.readUTF();
	}	
	
	public void write (DataOutput out) throws IOException
	{
		out.writeInt(x);
		out.writeInt(y);
		out.writeInt(dstPort);
		out.writeInt(sourcePort);
		out.writeInt(dstIP[0]);
		out.writeInt(dstIP[1]);
		out.writeInt(dstIP[2]);
		out.writeInt(dstIP[3]);
                out.writeInt(srcIP[0]);
                out.writeInt(srcIP[1]);
                out.writeInt(srcIP[2]);
                out.writeInt(srcIP[3]);
		out.writeUTF(date);
		out.writeUTF(time);
	}	

    public String toString()
    {
        StringBuilder str = new StringBuilder();

        str.append(dstIP[3]);
        str.append(".");
        str.append(dstIP[2]);
        str.append(".");
        str.append(dstIP[1]);
        str.append(".");
        str.append(dstIP[0]);
	str.append("-");
        str.append(x);
        str.append("-");
        str.append(y);
        str.append("-");
        str.append(dstPort);
        str.append("-");
        str.append(sourcePort);
        return (str.toString());
    }
}
