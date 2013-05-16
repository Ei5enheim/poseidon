/**
 * File name: Packet.java
 * Author: Rajesh Gopidi
 * Course: COMP 790-052
 * Final Project
 */

package poseidon.packet;

import java.io.*;
import java.util.*;

public class Packet 
{
    public int[] sourceIP;
    public int[] dstIP;
    public int sourcePort;
    public int dstPort;
    public byte[] payload;
    public static int MAX_PAYLOAD = 1460;
    public int payloadSize = 0;
    public String date;
    public String time;

    public Packet()
    {
        sourceIP = new int[4];
        dstIP = new int[4];
        payload = new byte[MAX_PAYLOAD];
	date = null;
	time = null;
    }

    public void setSrcIP (int[] IP)
    {
        System.arraycopy(IP, 0, sourceIP, 0, IP.length);
    }

    public void setDstIP (int[] IP)
    {
        System.arraycopy(IP, 0, dstIP, 0, IP.length);
    }

    public void setDstPort (int port)
    {
        dstPort = port;
    }

    public void setSrcPort (int port)
    {
        sourcePort = port;
    }

    public void setPayload (byte[] payload)
    {
        System.arraycopy(payload, 0, this.payload, 0, payload.length);
    }

    public void setFields (int[] srcIP, int[] dstIP, int srcPort, 
                            int dstPort, byte[] payload, int payloadSize)
    {
        sourceIP = srcIP;
        this.dstIP = dstIP;
        sourcePort = srcPort;
        this.dstPort = dstPort;
        this.payload = payload;
        this.payloadSize = payloadSize;
    }

    public void read (DataInput in)
    {
        try {
        // need to read the fields from the stream
            sourceIP[0] = in.readInt();
            sourceIP[1] = in.readInt();
            sourceIP[2] = in.readInt();
            sourceIP[3] = in.readInt();
            dstIP[0]= in.readInt();
            dstIP[1]= in.readInt();
            dstIP[2]= in.readInt();
            dstIP[3]= in.readInt();
            sourcePort = in.readInt();
            dstPort = in.readInt();
            for (int i = 0; i < payloadSize; i++)
                payload[i] = in.readByte();
        } catch (EOFException eof) {
            System.err.println("Encountered EOF Exception");
        } catch (IOException io) {
            System.err.println("Encountered IO Exception");
        }
    }

    public void write (DataOutput out)
    {
        try {
            out.writeInt(sourceIP[0]);
            out.writeInt(sourceIP[1]);
            out.writeInt(sourceIP[2]);
            out.writeInt(sourceIP[3]);
            out.writeInt(dstIP[0]);
            out.writeInt(dstIP[1]);
            out.writeInt(dstIP[2]);
            out.writeInt(dstIP[3]);
            out.writeInt(sourcePort);
            out.writeInt(dstPort);
            for (int i = 0; i < payloadSize; i++)
                out.write(payload[i]);
        } catch (EOFException eof) {
            System.err.println("Encountered EOF Exception");
        } catch (IOException io) {
            System.err.println("Encountered IO Exception");
        }
    }
}
