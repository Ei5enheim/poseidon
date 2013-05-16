/*
 * Author: Gopidi Rajesh                                         
 * File Name:  NeuronVectorWritable.java                                   
 * Course: COMP790-042                                           
 * Final Project                                                
 *                                                               
 */    

package poseidon.hadoop.io;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Writable;

public class NeuronVectorWritable implements Writable 
{
	public static int MAX_PAYLOAD_SIZE = 1460;
	public double[] vector;

	public NeuronVectorWritable()
	{
		vector = new double[MAX_PAYLOAD_SIZE];
	}

	public NeuronVectorWritable (int dummy)
	{
		vector = null;
	}

	public NeuronVectorWritable (double[] vector)
	{
		this.vector = vector;
	}

	public void set (double[] vector)
        {
                this.vector = vector;
        }

	public void write (DataOutput out) throws IOException
	{
		for (int i = 0; i < MAX_PAYLOAD_SIZE; i++)
			out.writeDouble(vector[i]);
	}

	public void readFields (DataInput in) throws IOException
	{
		 for (int i = 0; i < MAX_PAYLOAD_SIZE; i++)
                        vector[i] = in.readDouble();
	}

        public boolean equals (Object ob)
        {
        	NeuronVectorWritable nw = null;

                if (ob instanceof NeuronVectorWritable) {
                        nw = (NeuronVectorWritable) ob;

                        for (int i = 0; i < MAX_PAYLOAD_SIZE; i++) {
				if (nw.vector[i] != vector[i])
					return (false);
			}
			return (true);
                }
                return (false);
        }

	public String toString()
	{
		StringBuilder str = new StringBuilder();

		for (int i = 0; i < MAX_PAYLOAD_SIZE; i++) {
			str.append(vector[i]);
			str.append(", ");
		}
		return (str.toString());
	}
	
}
