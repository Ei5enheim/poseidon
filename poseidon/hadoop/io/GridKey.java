/*
 * Author: Gopidi Rajesh                                         
 * File Name:  GridKey.java                                   
 * Course: COMP790-042                                           
 * Final Project                                                
 *                                                               
 */    

package poseidon.hadoop.io;

import java.io.*;
import java.util.*;
import java.lang.Math;
import org.apache.hadoop.io.*;

public class GridKey implements WritableComparable<GridKey>
{
	public int x,y;
	public int port;
	
	public GridKey()
	{
		x = 0;
		y = 0;
		port = 0;
	}

	public GridKey (int x, int y, int port)
	{
		this.x = x;
		this.y = y;
		this.port = port;
	}

	public int hashCode()
	{
		/*
			double result = Math.pow (y,x);
			return (((int)result ^ port));		
		*/

		return ((x-1)*8 + y);
	}

	public int compareTo(GridKey key)
	{
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

	public boolean equals (Object ob)
	{
		GridKey key = null;

		if (ob instanceof GridKey) {
			key = (GridKey) ob;
			
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
	}
	
	public void write (DataOutput out) throws IOException
	{
		out.writeInt(x);
		out.writeInt(y);
		out.writeInt(port);
	}

    public String toString()
    {
        StringBuilder str = new StringBuilder();

        str.append(x);
	str.append(", ");
	str.append(y);
	str.append(", ");
	str.append(port);
        return (str.toString());
    }
}
