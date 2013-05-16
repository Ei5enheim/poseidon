
import java.io.*;
import java.util.*;

public class HeaderCheck
{
    public static long header = 0xA1B2C3D4;
    public static DataInputStream is;

    public static void main(String[] args) throws Exception
    {
        is = new DataInputStream(new FileInputStream(new File("outside_partitioned3")));
        byte[] data = new byte[48];
        is.read(data, 0, 24);
        //is.skip(24); 
        byte test;
        long num = ((data[3] & 0xFF) << 24) | ((data[2]& 0xFF) << 16) | ((data[1]& 0xFF) << 8) | (data[0] & 0xFF);

        long num_0 = ((data[0] & 0xFF) << 24) | ((data[1]& 0xFF) << 16) | ((data[2]& 0xFF) << 8) | (data[3] & 0xFF);    

        System.out.println("num1" + Long.toHexString(header));
        System.out.println("num_0 " + Long.toHexString(num_0));
        System.out.println("num1" + Long.toHexString(num));
        
        num = ((data[23] & 0xFF) << 24) | ((data[22]& 0xFF) << 16) | ((data[21]& 0xFF) << 8) | (data[20] & 0xFF);
        
        System.out.println("linktype" + Long.toHexString(num));        

        System.out.println("byte1: "+ Integer.toHexString(data[16]));
        System.out.println("byte2: "+ Integer.toHexString(data[17]));
        System.out.println("byte3: "+ Integer.toHexString(data[18]));
        System.out.println("byte4: "+ Integer.toHexString(data[19]));
        num = ((data[19] & 0xFF) << 24) | ((data[18]& 0xFF) << 16) | ((data[17]& 0xFF) << 8) | (data[16] & 0xFF);
        num = num & 0xFFFFFFFF;
        System.out.println("max length: " + num);
        //is.skipBytes(16);
        is.read(data,0,16);
        num = ((data[11] & 0xFF) << 24) | ((data[10]& 0xFF) << 16) | ((data[9]& 0xFF) << 8) | (data[8] & 0xFF);
        System.out.println("max length: " + num);
        is.read(data, 0, 14);
        int type = ((data[12] & 0xFF) << 8) | (data[13] & 0xFF);
        System.out.println("type- " + Long.toHexString(type));
        //is.skip(14);
        is.read(data, 0, 1);
        System.out.println("ip len- " + Integer.toHexString(((data[0] & 0x0F)*4)));
        is.skip(11);
        is.read(data, 0, 48);
        int ip3 = (data[0] & 0xFF);
        int ip2 = (data[1] & 0xFF);
        int ip1 = (data[2] & 0xFF);
        int ip0 = (data[3] & 0xFF);
        System.out.println("IP- " + ip3 + ", " +ip2 + ", " + ip1 + ", " +ip0 );
        int tcpLen = 0xA0;
        System.out.println("TCP length- " + Integer.toHexString(((tcpLen & 0xF0) >> 4)));
    }

}
