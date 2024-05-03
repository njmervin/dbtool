package org.yuyun.dbtool;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DataFile {
    private RandomAccessFile file = null;
    private DataOutput out = null;
    private DataInput in = null;
    private final ByteBuffer bb = ByteBuffer.allocate(8);

    public DataFile(String name, String mode) throws FileNotFoundException {
        file = new RandomAccessFile(name, mode);
        if(mode.contains("w"))
            out = file;
        in = file;
        bb.mark();
    }

    public DataFile(DataOutputStream os) {
        this.out = os;
        bb.mark();
    }

    public long getFilePointer() throws IOException {
        return file.getFilePointer();
    }

    public void seek(long pos) throws IOException {
        file.seek(pos);
    }

    public void close() throws IOException {
        if(file != null)
            file.close();
    }

    public void writeByte(byte value) throws IOException {
        out.writeByte(value);
    }

    public int readByte() throws IOException {
        return in.readByte();
    }

    public void writeShort(short value) throws IOException {
        bb.reset();
        bb.asShortBuffer().put(value);
        out.write(bb.array(), 0, 2);
    }

    public int readShort() throws IOException {
        bb.reset();
        in.readFully(bb.array(), 0, 2);
        return bb.getShort();
    }

    public void writeInteger(int value) throws IOException {
        bb.reset();
        bb.asIntBuffer().put(value);
        out.write(bb.array(), 0, 4);
    }

    public int readInteger() throws IOException {
        bb.reset();
        in.readFully(bb.array(), 0, 4);
        return bb.getInt();
    }

    public void writeLong(long value) throws IOException {
        bb.reset();
        bb.asLongBuffer().put(value);
        out.write(bb.array(), 0, 8);
    }

    public long readLong() throws IOException {
        bb.reset();
        in.readFully(bb.array(), 0, 8);
        return bb.getLong();
    }

    public void writeDouble(double value) throws IOException {
        bb.reset();
        bb.asDoubleBuffer().put(value);
        out.write(bb.array(), 0, 8);
    }

    public double readDouble() throws IOException {
        bb.reset();
        in.readFully(bb.array(), 0, 8);
        return bb.getDouble();
    }

    public void writeString(FieldType type, String value) throws IOException {
        bb.reset();

        int len = 0;
        byte[] data = null;
        if(value != null) {
            data = value.getBytes(StandardCharsets.UTF_8);
            len = data.length;
        }

        switch (type) {
            case SmallString:
                writeByte((byte) len);
                break;
            case MediumString:
                writeShort((short) len);
                break;
            case LongString:
                writeInteger(len);
                break;
        }

        if(len > 0)
            out.write(data);
    }

    public String readString(FieldType type) throws IOException {
        bb.reset();

        int len = 0;
        byte[] data = null;

        switch (type) {
            case SmallString:
                len = readByte();
                break;
            case MediumString:
                len = readShort();
                break;
            case LongString:
                len = readInteger();
                break;
        }

        if(len == 0)
            return null;
        else {
            data = new byte[len];
            in.readFully(data);
            return new String(data, StandardCharsets.UTF_8);
        }
    }


    public void writeDate(java.util.Date value) throws IOException {
        long x = 0;
        if(value != null) {
            x = value.getTime();
            x /= 3600 * 1000;
        }
        writeInteger((int) x);
    }

    public java.util.Date readDate() throws IOException {
        long x = readInteger();
        if(x == 0)
            return null;

        x *= 3600 * 1000;
        return new java.util.Date(x);
    }

    public void writeDateTime(java.util.Date value) throws IOException {
        long x = 0;
        if(value != null)
            x = value.getTime();
        writeLong(x);
    }

    public java.util.Date readDateTime() throws IOException {
        long x = readLong();
        if(x == 0)
            return null;
        else
            return new java.util.Date(x);
    }

    public void writeBinary(FieldType type, Blob blob) throws IOException, SQLException {
        if(blob == null) {
            switch (type) {
                case SmallBinary:
                    writeByte((byte) 0);
                    break;
                case MediumBinary:
                    writeShort((short) 0);
                    break;
                case LongBinary:
                    writeInteger(0);
                    break;
            }
        }
        else {
            InputStream is = blob.getBinaryStream();
            int len = is.available();
            switch (type) {
                case SmallBinary:
                    writeByte((byte) len);
                    break;
                case MediumBinary:
                    writeShort((short) len);
                    break;
                case LongBinary:
                    writeInteger(len);
                    break;
            }

            if(len > 0) {
                byte[] bytes = new byte[4096];
                while(len > 0) {
                    int n = is.read(bytes);
                    if(n == -1)
                        break;
                    if(n > 0) {
                        len -= n;
                        out.write(bytes, 0, n);
                    }
                }
            }
        }
    }

    public byte[] readBinary(FieldType type) throws IOException {
        int len = 0;
        switch (type) {
            case SmallBinary:
                len = readByte();
                break;
            case MediumBinary:
                len = readShort();
                break;
            case LongBinary:
                len = readInteger();
                break;
        }

        if(len == 0)
            return null;

        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }

    public void writeCompressBinary(byte[] bytes) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        GZIPOutputStream gzip;
        try {
            gzip = new GZIPOutputStream(os);
            gzip.write(bytes);
            gzip.close();
        } catch (IOException e) {
            throw e;
        }
        byte[] data = os.toByteArray();
        writeInteger(data.length);
        out.write(data);
    }

    public byte[] readCompressBinary() throws IOException {
        int len = readInteger();
        byte[] data = new byte[len];
        in.readFully(data);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ByteArrayInputStream is = new ByteArrayInputStream(data);
        try {
            GZIPInputStream ungzip = new GZIPInputStream(is);
            byte[] buffer = new byte[256];
            int n;
            while ((n = ungzip.read(buffer)) >= 0) {
                os.write(buffer, 0, n);
            }
        } catch (IOException e) {
            throw e;
        }

        return os.toByteArray();
    }

    public static void main(String[] args) {
        ByteBuffer bb4 = ByteBuffer.allocate(4);
        bb4.asIntBuffer().put(0x12345678);
        byte[] x = bb4.array();

        x[0] = 12;
        x[1] = 34;
        x[2] = 56;
        x[3] = 78;
        int m = bb4.getInt();
    }
}