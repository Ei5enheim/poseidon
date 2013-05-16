/**
 * File name: PacketReader.java
 * Author: Rajesh Gopidi
 * Course: COMP 790-052
 * Final Project
 *
 * This file contains the code of the functionality 
 * for reading TCP packets from a tcpdump file.
 * It is assumed that the default byte order is
 * is big-endian.
 */

package poseidon.packet;

import java.io.*;
import java.util.*;
import poseidon.packet.Packet;

public class PacketReader
{
    public DataInputStream in;
    private boolean reverse = false;
    private byte[] buffer;
    private int MAGIC_NUMBER = 0xa1b2c3d4;
    private int SWPD_MAGIC_NUMBER = 0xd4c3b2a1;
    private int MAX_PKT_LEN = 1520;
    private int MAX_PORT_RANGE = 1024;
    private LinkType linkType;
    private boolean caughtEOF = false;
    private Packet packet = null;
    private int currentPktLen = 0;
    private int vlanOverhead = 0;
    private int currentOffset = 0;
    private int currentPayloadSize = 0;
    private boolean noData = false;
    private int packetCount = 0;
    private boolean lookForNPkt = false;
    private int bytesProcessed = 0;
    private int HTTP = 80;
    private int FTP = 21;
    private int TELNET = 23;
    private int SMTP = 25;
    String[] str;

    public PacketReader()
    {

    } 
    public PacketReader(DataInputStream in) 
    {
        this.in = in;
        try {
            initialize(in);
        } catch (IOException io) {
            noData = true;
            System.err.println("caught IO Exception in Constructor");
        }
        packet = new Packet();
    }

    public void initialize (DataInputStream in) throws IOException
    {
        byte[] globalHeader = new byte[GLOBAL_HEADER_LEN];
        bytesProcessed += GLOBAL_HEADER_LEN;

        if (!readBytes(globalHeader, GLOBAL_HEADER_LEN)) {
            if (caughtEOF) {
                System.err.println("Skipping empty file");
                return;
            }
            System.err.println("throwing exception from initialize");
            throw new IOException("Couldn't read PCAP header");
        }

        this.reverse = checkMagicNumber(globalHeader);
        
        //System.out.println("reverse: " + reverse);
            
        int linkTypeVal = PacketReaderUtils.getUInt(globalHeader,
                                                    DLINK_TYPE_OFFSET,
                                                    reverse);
        //System.out.println("linktypeval= "+ linkTypeVal);
        linkType = getLinkType(linkTypeVal);

        if (linkType == null)
        {
            System.err.println("linktype is not supported");
            throw new IOException("Unsupported link type: " + linkTypeVal);
        }

        buffer = new byte[MAX_PKT_LEN];   
    } 

    public long getProgress()
    {
        return (bytesProcessed);
    }

    public boolean hasOneMore()
    {

	boolean rt = false;
        //System.out.println("PacketCount: " + packetCount);
        if (noData)
            return (false);
    
        if (linkType == LinkType.LINUX_SLL) {
            // will ignore it for now
            return (false);
        } else if (linkType == LinkType.ETHERNET) {
	    while (!(rt = parseHelper()) && lookForNPkt) {
	    }
	    return (rt);
	}
        return (true);
    }

    public boolean parseHelper ()
    {
	lookForNPkt = false;
	packet.payloadSize = 0;
	currentOffset = 0;
	currentPayloadSize = 0;
	currentPktLen = 0;
	try {
	    if (!parsePCAPHeader())
		return (false);

	    return (parsePacket());
        } catch (IOException io) {
	    System.err.println("caught IO Exception in hasOneMore");
	}
	return (true);
    } 

    public Packet nextPacket ()
    {
        return (packet);
    }

    public boolean parsePCAPHeader() throws IOException
    {
        bytesProcessed += RECORD_HEADER_LEN;
        if (!readBytes(buffer, RECORD_HEADER_LEN)) {
            if (caughtEOF) {
                System.out.println("caught EOFException");
                return (false);
            }
            throw new IOException("Unable to read the record header ");
        }
	str = PacketReaderUtils.getTimeStamp(buffer, PACKET_TSTAMP_SECS_OFFSET, reverse);
        currentPktLen = PacketReaderUtils.getUInt(buffer, PACKET_LEN_FOFFSET, reverse); 
	packet.date = str[0];
	packet.time = str[1];
        return (true);
    }

    public boolean parsePacket() throws IOException
    {
        bytesProcessed += currentPktLen;
        if (!readBytes(buffer, currentPktLen)) {
            if (caughtEOF) {
                System.out.println("caught EOFException");
                return (false);
            }
            throw new IOException("Unable to read packet");
        }

        if (!parseEthernetHeader()) {
            //System.out.println("unable to parseEthernet Header");
            lookForNPkt = true;
            return (false); 
        }

        if (!parseTCPIPHeader()) {
            //System.out.println("unable to parseTCP Header");
            lookForNPkt = true;
            return (false);
        }
    
        return (true);
    }

    public boolean parseEthernetHeader()
    {
        int etherTypeVal = PacketReaderUtils.getUInt(buffer, ETHERTYPE_OFFSET);
                                                        
        EtherType etherType = getEtherType(etherTypeVal);
        if (etherType == EtherType.VLANTAG) {
            etherTypeVal = PacketReaderUtils.getUInt(buffer,
                                                        VLANTAG_ETHERTYPE_OFFSET);
            etherType = getEtherType(etherTypeVal);
            // need to move forward by 4 bytes because of VLAN tag in the packet
            currentOffset += 4;
        }
        // skipping the current packet and looking ahead using recursion
        if (etherType != EtherType.IPV4) {
            //System.out.println("Not an IP packet");
            return (false);
        }

        currentOffset += ETHERNET_HEADER;
        return (true);
    }

    public boolean parseTCPIPHeader ()
    {
        packetCount++;
        int length = PacketReaderUtils.getIPHLen (buffer, currentOffset);
        int prtclValue = PacketReaderUtils.getByteValue (buffer, currentOffset + 
                                                          IP_PROTOCOL_OFFSET);
        if (getIPProtocol(prtclValue) != IPProtclType.TCP) {
            //System.out.println("Not a TCP Packet");
            return (false);
        }

        currentPayloadSize = PacketReaderUtils.getUInt(buffer, currentOffset +
                                                       IP_PACKET_LEN_OFFSET);

        currentPayloadSize -= length;

        // we retrieve both source and destination IP in one call
        PacketReaderUtils.getIP(buffer, currentOffset + IP_SRC_OFFSET, packet);
        currentOffset += length;
        packet.sourcePort = PacketReaderUtils.getPort(buffer, currentOffset + TCP_SRC_PORT);
        packet.dstPort = PacketReaderUtils.getPort(buffer, currentOffset + TCP_DST_PORT);


	//Need to reconsider this;
	if ((packet.dstPort != HTTP) &&
	    (packet.dstPort != TELNET) &&
            (packet.dstPort != SMTP) &&
            (packet.dstPort != FTP))
		return (false);
        /*	
	if (packet.dstPort > MAX_PORT_RANGE) {
	    return (false);
	} */
       
        length = PacketReaderUtils.getTCPHLen(buffer, currentOffset + DATA_OFFSET);
        currentPayloadSize -= length;
        currentOffset += length;
        
        //packetCount++;
        if (currentPayloadSize < 1) {
            //System.out.println("No payload");
            return (false);
        }
        PacketReaderUtils.getPayload(buffer, currentOffset, currentPayloadSize, packet);
        return (true);
    }

    public IPProtclType getIPProtocol (int type)
    {
        switch (type) {
            case 0x06:
                return (IPProtclType.TCP);
            case 0x01:
            case 0x02:
            case 0x04:
            case 0x11:
            default :
        }
        return (null);
    }

    public boolean readBytes (byte[] buffer)
    {
        try {
            in.readFully(buffer);
            return (true);
        } catch (EOFException eof) {
            System.out.println("1caught EOF execption");
            caughtEOF = true;
        } catch (IOException e) {
            System.out.println("1caught IO execption");
            e.printStackTrace();
        }
        return (false);
    }

    public boolean readBytes (byte[] buffer, int length)
    {
        try {
            in.readFully(buffer, 0, length);
            return (true);
        } catch (EOFException eof) {
            System.out.println("caught EOF execption");
            caughtEOF = true;
        } catch (IOException e) {
            System.out.println("caught IO execption");
            e.printStackTrace();
        }
        return (false);
    }


    public LinkType getLinkType(int linkTypeVal)
    {
        switch (linkTypeVal) {
        case 1:
            return (LinkType.ETHERNET);
        case 113:
            System.err.println("Found linux cooked linktype");
            return (LinkType.LINUX_SLL);
        case 0:
        case 6:
        case 105:
        case 228:
        case 229:
        default:
       } 
        return (null);
    }

    public EtherType getEtherType(int type)
    {
        switch (type) {
            case 0x0800:
                return (EtherType.IPV4);
            case 0x08100:
                return (EtherType.VLANTAG);
            case 0x86DD:
            case 0x88CC:
            default:
        }
        return (null);
    }

    public boolean checkMagicNumber (byte[] globalHeader) throws IOException
    {
        int rvalue = PacketReaderUtils.getUInt(globalHeader);
        
        System.out.println("magin number: "+ Integer.toHexString(rvalue));
        
        if (rvalue == MAGIC_NUMBER) {
            return (true); 
        } else if (rvalue == SWPD_MAGIC_NUMBER) {
            
        } else {
            throw new IOException("Not a PCAP file (Couldn't find magic number)");
        }
        return (false);
    }

    // all values in bytes
    private int GLOBAL_HEADER_LEN = 24;
    private int MAGIC_NUM_OFFSET = 0;
    private int MAGIC_NUM_LEN = 4;
    private int MAX_LEN_OFFSET = 16;
    private int MAX_LEN_FLEN = 4;
    private int DLINK_TYPE_OFFSET = 20;
    private int DLINK_TYPE_LEN = 4;
    private int RECORD_HEADER_LEN = 16;
    private int PACKET_TSTAMP_SECS_OFFSET = 0;
    private int PACKET_TSTAMP_USECS_OFFSET = 4;
    private int PACKET_LEN_FOFFSET = 8;
    private int PACKET_LEN_FLEN = 4;
    private int ETHERNET_HEADER = 14;
    private int ETHERTYPE_OFFSET = 12;
    private int ETHERTYPE_LEN = 2;
    private int VLANTAG_ETHERTYPE_OFFSET = 16;
    private int IP_HEADER_LEN_OFFSET = 0;// need to access the last four bits
    private int IP_PACKET_LEN_OFFSET = 2;
    private int IP_PROTOCOL_OFFSET = 9;
    private int IP_SRC_OFFSET = 12;
    private int IP_DST_OFFSET = 16;
    private int TCP_SRC_PORT = 0;
    private int TCP_DST_PORT = 2;
    private int DATA_OFFSET = 12;// this is a four bit field and size is in terms of 32 bits

    // case for linux cooked headers
    private int PACKET_TYPE_OFFSET = 0;
    private int PACKET_TYPE_LEN = 2;
    private int LINUX_DLINK_TYPE_OFFSET = 2;
    private int LINUX_DLINK_TYPE_LEN = 2;
    private int LINUX_ETHERTYPE_OFFSET = 14;
    private int LINUX_ETHERTYPE_LEN = 2;

    private enum LinkType
    {
        NULL, //0
        ETHERNET, //1
        IEEE802_5, //6
        IEEE802_11, // 105
        LINUX_SLL, //113
        IPV4, // 228
        IPV6 // 229
    }

    private enum EtherType
    {
        IPV4, //0x0800
        VLANTAG, //0x08100
        IPV6, //0x86DD
        LLDP, //0x88CC
    }

    private enum IPProtclType
    {
        ICMP, //0x01
        IGMP, //0x02
        IPV4, //0x04
        TCP,  //0x06
        UDP,  //0x11 
    }
}
