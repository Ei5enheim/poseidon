/**
 * Test program to read packets from .tcpdump file
 *
 */

import org.jnetpcap.*;
import org.jnetpcap.packet.*;
import org.jnetpcap.protocol.*; 
import org.jnetpcap.protocol.tcpip.*;
import org.jnetpcap.protocol.network.*;
import org.jnetpcap.protocol.network.Ip4.*;
import org.jnetpcap.nio.*;
import java.util.*;
import java.io.*;
import java.lang.reflect.*;

class PacketRead 
{
    StringBuilder errbuf;
    Pcap capObj;
    PcapPacket packet;

    public PacketRead()
    {
        // creates a string object with capacity of 16 chars.
        errbuf = new StringBuilder();
        capObj = Pcap.openOffline("outside_partitioned3", errbuf);
        packet = new PcapPacket(JMemory.POINTER);
    }

    public void readCapture() 
    {
        while (capObj.nextEx(packet) == Pcap.NEXT_EX_OK) {
            PcapPacket copy = new PcapPacket(packet);
            if (copy.hasHeader(Ip4.ID)) {
                Ip4 ip = copy.getHeader(new Ip4());
                byte[] sourceIP = ip.source();
                byte[] destIP = ip.destination();
            
                if (copy.hasHeader(Tcp.ID)) {
                    int i = 0;
                
                    for (byte b: sourceIP) {
                        i = 0x0000007F;
                        if (b < 0) {
                            i = i & b;
                            i = (0x00000080) | i; 
                            System.out.println(i);
                        } else {
                            System.out.println(b);
                        }
                    }
                    Tcp tcp = copy.getHeader(new Tcp());
                    byte[] bytes = tcp.getPayload();
                    if (tcp.getPayloadLength() > 0) {
                        System.out.println("srcport= "+ tcp.source()+" destinationPort = "+tcp.destination()); 
                        break;
                    }
                }
            }
        }
    }

    public static void setDPath() throws Exception
    {
        String path = System.getProperty("java.library.path");
        System.setProperty("java.library.path", path + ":/home/rajesh/Documents");
        Field fieldSysPath = ClassLoader.class.getDeclaredField( "sys_paths" );
        fieldSysPath.setAccessible( true );
        fieldSysPath.set( null, null );
        //System.out.println("run time path - "+ System.getProperty("java.library.path"));


    }

    public static void main (String[] args) throws Exception
    {
        File f = new File("libjnetpcap.so");  
        File currentDirectory = new File(new File(".").getAbsolutePath());
        System.out.println(currentDirectory.getCanonicalPath());
        System.out.println(currentDirectory.getAbsolutePath());
        if (f.exists())
            System.out.println("Fuck you this doesnt work");  
        setDPath();
        PacketRead pr = new PacketRead();
        int i = 0;
        while (i < 2) {
            pr.readCapture();
            i++;
        }
    }
} 
