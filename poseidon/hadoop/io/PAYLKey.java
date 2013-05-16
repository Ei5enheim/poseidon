/*
 * Author: Gopidi Rajesh                                         
 * File Name: PAYLKey.java                                   
 * Course: COMP790-042                                           
 * Final Project                                                
 *                                                               
 */    

package poseidon.hadoop.io;

import java.io.*;
import java.util.*;
import java.lang.Math;
import org.apache.hadoop.io.*;

public class PAYLKey implements WritableComparable<PAYLKey>
{
	public int x,y;
	public int port;
	public int[] IP;
	
	public PAYLKey()
	{
		x = 0;
		y = 0;
		port = 0;
		IP = new int[4];
	}

	public PAYLKey (int x, int y)
	{
		this.x = x;
		this.y = y;
		port = 0;
		IP = null;
	}

	public PAYLKey (int x, int y, int port, int[] IP)
	{
		this.x = x;
		this.y = y;
		this.port = port;
		this.IP = IP;
	}

	public int hashCode()
	{
		int i = 0;
		i = IP[0] + IP[1]*255 + IP[2] *255*255;
		//need to return to this
		return (i+(x-1)*8 + y +port);
	}

	public int compareTo(PAYLKey key)
	{
		String ip1 = Arrays.toString(key.IP);
		String ip2 = Arrays.toString(IP);
		if (ip2.equals(ip1)) {
			if (port > key.port)
				return (1);
			else if (port < key.port)
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
		PAYLKey key = null;

		if (ob instanceof PAYLKey) {
			key = (PAYLKey) ob;
			
			if (this.compareTo(key) == 0)
				return (true);
		}
		return (false);
	}
	
	public void readFields (DataInput in) throws IOException
	{
		x = in.readInt();
		y = in.readInt();
		port = in.readInt();
		IP[0] = in.readInt();
		IP[1] = in.readInt();
		IP[2] = in.readInt();
		IP[3] = in.readInt();
	}	
	
	public void write (DataOutput out) throws IOException
	{
		out.writeInt(x);
		out.writeInt(y);
		out.writeInt(port);
		out.writeInt(IP[0]);
		out.writeInt(IP[1]);
		out.writeInt(IP[2]);
		out.writeInt(IP[3]);
	}	

    public String toString()
    {
        StringBuilder str = new StringBuilder();

        str.append(IP[3]);
        str.append(".");
        str.append(IP[2]);
        str.append(".");
        str.append(IP[1]);
        str.append(".");
        str.append(IP[0]);
	str.append("-");
        str.append(x);
        str.append("-");
        str.append(y);
        str.append("-");
        str.append(port);
        return (str.toString());
    }
}
