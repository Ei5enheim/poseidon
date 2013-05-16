/*
 * Author: Gopidi Rajesh                                         
 * File Name:  PacketWritable.java                                   
 * Course: COMP790-042                                           
 * Final Project                                                
 *                                                               
 */    

package poseidon.hadoop.io;

import org.apache.hadoop.io.Writable;
import poseidon.packet.Packet;
import java.io.*;
import java.util.*;

public class PacketWritable implements Writable
{
    public Packet packet;

    public PacketWritable()
    {
        packet = null;
    }
    
    public PacketWritable(Packet packet)
    {
        this.packet = packet;
    }

    public void readFields (DataInput in) throws IOException
    {
        if (packet != null) 
            packet.read(in);
    }

    public void write (DataOutput out) throws IOException
    {
        if (packet != null)
            packet.write(out); 
    }
    
    public boolean equals (Object obj)
    {
	PacketWritable pw = null;

	if (obj instanceof PacketWritable) {
		pw = (PacketWritable) obj; 
	} else {
		return (false);
	}
		
	if ((packet == null) && (this.packet == pw.packet))
		return (true);
	else if (packet == null)
		return (false);

	if (this.packet.payloadSize != pw.packet.payloadSize)
		return (false);

	for (int i = 0; i < packet.payloadSize; i++)
	{
		if (this.packet.payload[i]  != pw.packet.payload[i])
	 		return (false);
	}
	return (true);
    }
    
    public int convertToInt(byte value)
    {
	    int i = 0x0000007F;
	    i = i & value;
	    i = (0x00000080) | i;
	    return (i);
    }

    public String toString()
    {
	StringBuilder str = new StringBuilder();

        if (packet == null)
                return (str.toString());

        for (int i = 0; i < packet.payloadSize; i++)
        {
                if (this.packet.payload[i] < 0)
        		str.append(convertToInt(packet.payload[i]));
		str.append((int)packet.payload[i]);
		str.append(", ");
	}
	
        return (str.toString());
    } 
}
