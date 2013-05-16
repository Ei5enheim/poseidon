/*
 * Author: Gopidi Rajesh                                         
 * File Name:  PAYLAvgFWritable.java                                   
 * Course: COMP790-042                                           
 * Final Project                                                
 *                                                               
 */    

package poseidon.hadoop.io;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Writable;

public class PAYLAvgFWritable implements Writable 
{
	public static int FREQ_ARRAY_SIZE = 256;
	public double[] avgByteFreq;

	public PAYLAvgFWritable()
	{
		avgByteFreq = new double[FREQ_ARRAY_SIZE];
	}

	public PAYLAvgFWritable (double[] freq)
	{
		this.avgByteFreq = freq;
	}

	public PAYLAvgFWritable (int dummy)
	{
		this.avgByteFreq = null;
	}

	public void set (double[] avgByteFreq)
	{
                this.avgByteFreq = avgByteFreq;
	}

	public void write (DataOutput out) throws IOException
	{
		for (int i = 0; i < FREQ_ARRAY_SIZE; i++)
			out.writeDouble(avgByteFreq[i]);
	}

	public void readFields (DataInput in) throws IOException
	{
		 for (int i = 0; i < FREQ_ARRAY_SIZE; i++)
                        avgByteFreq[i] = in.readDouble();
	}

        public boolean equals (Object ob)
        {
        	PAYLAvgFWritable payl = null;

                if (ob instanceof PAYLAvgFWritable) {
                        payl = (PAYLAvgFWritable) ob;

                        for (int i = 0; i < FREQ_ARRAY_SIZE; i++) {
				if (payl.avgByteFreq[i] != avgByteFreq[i]) 
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
