/*
 * Author: Gopidi Rajesh                                         
 * File Name:  PAYLTestVWritable.java                                   
 * Course: COMP790-042                                           
 * Final Project                                                
 *                                                               
 */    

package poseidon.hadoop.io;

import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.*;

public class PAYLTestVWritable implements Writable
{
	public short [] freq;
	public int payloadSize;
	public int srcPort;
	public int[] srcIP;
        public String date;
        public String time;
	public static int SIZE = 256;

	public PAYLTestVWritable()
	{
		freq = new short[SIZE];
		srcIP = new int[4];
	}

        public PAYLTestVWritable(int dummy)
        {
        }

        public PAYLTestVWritable(short[] freq)
        {
                this.freq = freq;
        }

	public void readFields (DataInput in) throws IOException
	{
		for (int i = 0; i < SIZE; i++) {
			freq[i] = in.readShort();
		}
		payloadSize = in.readInt();
		srcPort = in.readInt();
		srcIP[0] = in.readInt();
                srcIP[1] = in.readInt();
                srcIP[2] = in.readInt();
                srcIP[3] = in.readInt();
		date = in.readUTF();
		time = in.readUTF();
        }

        public void write (DataOutput out) throws IOException
        {
                for (int i = 0; i < SIZE; i++)
                        out.writeShort(freq[i]);

		out.writeInt(payloadSize);
		out.writeInt(srcPort);
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
		
		str.append(freq[0]);
                for (int i = 1; i < SIZE; i++) {
			str.append(", ");
                        str.append(freq[i]);
                }
                return (str.toString());
        }

        public boolean equals (Object ob)
        {
                PAYLTestVWritable wr = null;

                if (ob instanceof PAYLTestVWritable) {
                        wr = (PAYLTestVWritable) ob;

                        for (int i = 0; i < SIZE; i++) {
                                if (wr.freq[i] != freq[i])
                                        return (false);
                        }
			return (true);
                }
                return (false);
        }
}
