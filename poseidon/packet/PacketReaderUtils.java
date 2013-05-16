/**                                                     
 * File name: PacketReaderUtils.java
 * Author: Rajesh Gopidi
 * Course: COMP 790-052
 * Final Project
 *      
 * This file contains the definitions for the 
 * utility routines required to parse a TCP packet.
 * It is assumed that the default byte order is
 * is big-endian.
 */     

package poseidon.packet;

import java.util.*;
import java.text.*;

public class PacketReaderUtils 
{
    static String[] ret = new String[2];
    static TimeZone tz = TimeZone.getTimeZone("GMT-4");
    static Calendar cal  = Calendar.getInstance(tz);
    static SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy:HH:mm:ss");
    static String date;
    static String time;
    static StringBuilder string = new StringBuilder();

    public static int getUInt (byte[] buffer)
    {   
        // take 4 bytes from the buffer, going for the big-endian
        return (getUInt(buffer, 0, true));
    }

    public static int getUInt (byte[] buffer, int offset, boolean reverse)
    {
        int value = 0;
        // take 4 bytes from the buffer
        if (reverse)
            value = ((buffer[offset] & 0xFF) << 24) | ((buffer[offset +1] & 0xFF) << 16) |
                ((buffer[offset+2] & 0xFF) << 8) | (buffer[offset+3] & 0xFF);
        else 
            value = ((buffer[offset + 3] & 0xFF) << 24) | ((buffer[offset + 2] & 0xFF) << 16) |
                ((buffer[offset+1] & 0xFF) << 8) | (buffer[offset] & 0xFF);

        return (value);
    }

    public static long getULong (byte[] buffer, int offset, boolean reverse)
    {
        long constant = 0x80000000L;
        int temp = getUInt(buffer, offset, reverse);
	long ret = 0;

        if (temp < 0)
        {
                ret = temp & 0x7FFFFFFF;
                ret = ret | constant;
        } else {
                ret = (long) temp;
        }
        return (ret);
    }

    public static String[] getTimeStamp (byte[] buffer, int offset, boolean reverse)
    {
	long secs = 0;
	String str = null;
	long msecs = 0;
	long usecs = 0;

	long timeInSecs = getULong(buffer, offset, reverse);
	//System.out.println("****time in timeInSecs ***" + timeInSecs);
	usecs = getULong(buffer, offset + 4, reverse);
	//System.out.println("time in usecs" + usecs);
        secs = usecs/(1000*1000);
	//System.out.println("time in secs" + secs);
        usecs -= secs*1000*1000;
	//System.out.println("time in usecs" + usecs);
        msecs = usecs/1000;
	//System.out.println("time in msecs" + msecs);
        usecs -= msecs * 1000;
	//System.out.println("time in usecs" + usecs);
        if (secs > 0)
        {
		timeInSecs += secs;
		secs = 0;
	}

	timeInSecs = timeInSecs * 1000;
        cal.setTimeInMillis(timeInSecs);
	//System.out.println("total time" + timeInSecs);
        format.setTimeZone(tz);
        date = format.format(cal.getTime());
        
        time = date.substring(date.indexOf(':')+1);
        date = date.substring(0, date.indexOf(':'));
	//System.out.println(string.toString());
        string.append(time);
        string.append(':');
        string.append(Long.toString(msecs));
        string.append(':');
        string.append(Long.toString(usecs));
        time = string.toString();
        string.delete(0, string.length());
        ret[0] = date;
        ret[1] = time;

        return (ret);
    }

    public static int getByteValue (byte[] buffer, int offset)
    {   
        return (buffer[offset] & 0xFF);
    }

    public static int getUInt (byte[] buffer, int offset)
    {
        return (((buffer[offset] & 0xFF) << 8) | (buffer[offset + 1] & 0xFF));
    }

    public static int getIPHLen (byte[] buffer, int offset)
    {
        return ((buffer[offset] & 0x0F) * 4);
    }

    public static void getIP (byte[] buffer, int offset, Packet packet)
    {
        if (packet.sourceIP != null)
        {
            packet.sourceIP[3] = buffer[offset++] & 0xFF;
            packet.sourceIP[2] = buffer[offset++] & 0xFF;
            packet.sourceIP[1] = buffer[offset++] & 0xFF;
            packet.sourceIP[0] = buffer[offset++] & 0xFF;
        }    

        if (packet.dstIP != null)
        {
            packet.dstIP[3] = buffer[offset++] & 0xFF;
            packet.dstIP[2] = buffer[offset++] & 0xFF;
            packet.dstIP[1] = buffer[offset++] & 0xFF;
            packet.dstIP[0] = buffer[offset++] & 0xFF;
        }
    }

    public static int getPort (byte[] buffer, int offset) 
    {
        return (((buffer[offset] & 0xFF) << 8) | (buffer[offset+1] & 0xFF));
    }

    public static int getTCPHLen (byte[] buffer, int offset)
    {
        return (((buffer[offset] & 0xF0) >> 4) * 4);
    }

    public static void getPayload(byte[] buffer, int offset,
            int length, Packet packet)
    {
        System.arraycopy(buffer, offset, packet.payload, 0, length);
        packet.payloadSize = length;
    }
}
