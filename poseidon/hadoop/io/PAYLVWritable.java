/*
 * Author: Gopidi Rajesh                                         
 * File Name:  PAYLVWritable.java                                   
 * Course: COMP790-042                                           
 * Final Project                                                
 *                                                               
 */    

package poseidon.hadoop.io;

import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.*;

public class PAYLVWritable implements Writable
{
	public short [] freq;
	public int payloadSize;
	public static int SIZE = 256;

	public PAYLVWritable()
	{
		freq = new short[SIZE];
	}

        public PAYLVWritable(int dummy)
        {
                freq = null;
        }

        public PAYLVWritable(short[] freq)
        {
                this.freq = freq;
        }

        public PAYLVWritable(short[] freq, int payloadSize)
        {
                this.freq = freq;
		this.payloadSize = payloadSize;
        }

	public void readFields (DataInput in) throws IOException
	{
		for (int i = 0; i < SIZE; i++) {
			freq[i] = in.readShort();
		}
		payloadSize = in.readInt();
        }

        public void write (DataOutput out) throws IOException
        {
                for (int i = 0; i < SIZE; i++)
                        out.writeShort(freq[i]);

		out.writeInt(payloadSize);

        }

        public String toString()
        {
                StringBuilder str = new StringBuilder();
		
		str.append(freq[0]);
                for (int i = 1; i < SIZE; i++) {
			str.append(", ");
                        str.append(freq[i]);
                }
		str.append("-");
		str.append(payloadSize);
                return (str.toString());
        }

        public boolean equals (Object ob)
        {
                PAYLVWritable wr = null;

                if (ob instanceof PAYLVWritable) {
                        wr = (PAYLVWritable) ob;

                        for (int i = 0; i < SIZE; i++) {
                                if (wr.freq[i] != freq[i])
                                        return (false);
                        }
			if (payloadSize != wr.payloadSize)
				return (false);

			return (true);
                }
                return (false);
        }
}
