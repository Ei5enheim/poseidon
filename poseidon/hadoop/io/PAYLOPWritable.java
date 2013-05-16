/*
 * Author: Gopidi Rajesh                                         
 * File Name:  PAYLOPWritable.java                                   
 * Course: COMP790-042                                           
 * Final Project                                                
 *                                                               
 */    

package poseidon.hadoop.io;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Writable;

public class PAYLOPWritable implements Writable 
{
	public static int FREQ_ARRAY_SIZE = 256;
	public double[] avgByteFreq;
	public double[] SD;

	public PAYLOPWritable()
	{
		avgByteFreq = new double[FREQ_ARRAY_SIZE];
		SD = new double[FREQ_ARRAY_SIZE];
	}

	public PAYLOPWritable (double[] freq, double[] SD)
	{
		this.avgByteFreq = freq;
		this.SD = SD;
	}

	public PAYLOPWritable (int dummy)
	{
		this.avgByteFreq = null;
		this.SD = null;
	}

	public void set (double[] avgByteFreq, double[] SD)
	{
                this.avgByteFreq = avgByteFreq;
                this.SD = SD;
	}

	public void write (DataOutput out) throws IOException
	{
		for (int i = 0; i < FREQ_ARRAY_SIZE; i++)
			out.writeDouble(avgByteFreq[i]);

                for (int i = 0; i < FREQ_ARRAY_SIZE; i++)
                        out.writeDouble(SD[i]);
	}

	public void readFields (DataInput in) throws IOException
	{
		 for (int i = 0; i < FREQ_ARRAY_SIZE; i++)
                        avgByteFreq[i] = in.readDouble();

                 for (int i = 0; i < FREQ_ARRAY_SIZE; i++)
                        SD[i] = in.readDouble();
	}

        public boolean equals (Object ob)
        {
        	PAYLOPWritable payl = null;

                if (ob instanceof PAYLOPWritable) {
                        payl = (PAYLOPWritable) ob;

                        for (int i = 0; i < FREQ_ARRAY_SIZE; i++) {
				if ((payl.avgByteFreq[i] != avgByteFreq[i]) ||
				    (payl.SD[i] != SD[i]))
					return (false);
			}
			return (true);
                }
                return (false);
        }

	public String toString()
	{
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < FREQ_ARRAY_SIZE; i++) {
			str.append(avgByteFreq[i]);
			str.append(", ");
		}
		return (str.toString());
	}
	
}
