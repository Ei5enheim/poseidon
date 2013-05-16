/*
 * Author: Gopidi Rajesh                                         
 * File Name:  BatchSOMUpdateWritable.java                                   
 * Course: COMP790-042                                           
 * Final Project                                                
 *                                                               
 */    

package poseidon.hadoop.io;

import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.*;

public class BatchSOMUpdateWritable implements Writable
{
	public double[] num;
	public double den;
	public static int MAX_PAYLOAD_SIZE = 1460;
	public static double MAGICNUMBER = -99999;

	public BatchSOMUpdateWritable()
	{
		num = new double[MAX_PAYLOAD_SIZE];
		den = 0;
	}

        public BatchSOMUpdateWritable(double den)
        {
                num = null;
                den = 0;

        }

        public BatchSOMUpdateWritable(double[] num, double den)
        {
                this.num = num;
                this.den = den;
        }

	public void readFields (DataInput in) throws IOException
	{
		double count = 0;

		for (int i = 0; i < MAX_PAYLOAD_SIZE; i++) {
			count = in.readDouble();
			if (count == MAGICNUMBER) 
				break;
			num[i] = count; 
		}
		try {
                den = in.readDouble();
		} catch (ArrayIndexOutOfBoundsException aioe){
			System.err.println("\n ****** count = " + count);
			throw new IOException("this isn't working :(");
		}
        }

	public int findLastIndex()
	{
		int count = 0;

		for (int i = 0; i < MAX_PAYLOAD_SIZE; i++) {
			if (num[i] == 0) {
				count++;
			} else {
				count = 0;
			}
		}
		return (count);
	}

        public void write (DataOutput out) throws IOException
        {
		int count = findLastIndex();
                for (int i = 0; i < (MAX_PAYLOAD_SIZE - count); i++)
                        out.writeDouble(num[i]);

		if (count > 0)
			out.writeDouble(MAGICNUMBER);	

                out.writeDouble(den);
        }

        public String toString()
        {
                StringBuilder str = new StringBuilder();

                for (int i = 0; i < MAX_PAYLOAD_SIZE; i++) {
                        str.append(num[i]);
                        str.append(", ");
                }
		str.append(den);
                return (str.toString());
        }

        public boolean equals (Object ob)
        {
                BatchSOMUpdateWritable bsw = null;

                if (ob instanceof BatchSOMUpdateWritable) {
                        bsw = (BatchSOMUpdateWritable) ob;

                        for (int i = 0; i < MAX_PAYLOAD_SIZE; i++) {
                                if (bsw.num[i] != num[i])
                                        return (false);
                        }
			if (bsw.den == den)
                        	return (true);
                }
                return (false);
        }

}
