package ypf412.storm.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * This wrapper class customs its own serialized function (writeObject and readObject) for byte array.
 * As we know, Storm will serialize tuples between spout and bolt, however by default the serialization 
 * of java byte array is so inefficient! This is an extremely huge performance tuning especially when 
 * dealing with BIG DATA from HBase cluster.
 * A rough test on production environment shows it could improve QPS from 6k to 24k when transferring 
 * tuples between spout and bolt (a tuple is about 1024 bytes).
 * 
 * @author jiuling.ypf
 *
 */
public class StreamData implements Serializable {
	
   private static final long serialVersionUID = 4755376588252668977L;
   
   private byte[] data;

   public StreamData(byte[] data) {
      this.data = data;
   }

   public byte[] getData() {
      return data;
   }

   private void writeObject(ObjectOutputStream out) throws IOException {
      out.writeInt(data.length);
      out.write(data, 0, data.length);
   }

   private void readObject(ObjectInputStream in) throws IOException {
      int len = in.readInt();
      data = new byte[len];
      int start = 0;
      while(len > 0)
      {
         int read =in.read(data,start,len);
         if(read <=0)
         {
            throw new IOException("Read Failed!");
         }
         start += read;
         len -= read;
      }
   }
}
