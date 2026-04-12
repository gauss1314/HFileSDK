package io.hfilesdk.converter.javaimpl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import org.apache.hadoop.hbase.util.JVM;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.GzipCodec;

/**
 * GZip codec with deterministic, reusable stream state and an explicit level.
 *
 * <p>We always use the JDK GZIPOutputStream framing path so the pure-Java writer
 * and the JNI/C++ writer can target the same byte layout.
 */
public final class FixedLevelGzipCodec extends GzipCodec {

    static final String LEVEL_KEY = "io.hfilesdk.gz.level";
    static final int DEFAULT_LEVEL = 1;
    private static final int DEFAULT_BUFFER_SIZE = 512;

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
        return new FixedLevelGzipOutputStream(out, compressionLevel());
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
        throws IOException {
        return createOutputStream(out);
    }

    @Override
    public Compressor createCompressor() {
        return null;
    }

    @Override
    public Class<? extends Compressor> getCompressorType() {
        return null;
    }

    private int compressionLevel() {
        if (getConf() == null) {
            return DEFAULT_LEVEL;
        }
        return getConf().getInt(LEVEL_KEY, DEFAULT_LEVEL);
    }

    private static final class FixedLevelGzipOutputStream extends CompressorStream {
        private final ResettableGzipStream gzipStream;

        private FixedLevelGzipOutputStream(OutputStream out, int level) throws IOException {
            super(new ResettableGzipStream(out, level));
            this.gzipStream = (ResettableGzipStream) this.out;
        }

        @Override
        public void close() throws IOException {
            out.close();
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] data, int offset, int length) throws IOException {
            out.write(data, offset, length);
        }

        @Override
        public void finish() throws IOException {
            gzipStream.finish();
        }

        @Override
        public void resetState() throws IOException {
            gzipStream.resetState();
        }
    }

    private static final class ResettableGzipStream extends DeflaterOutputStream {
        private static final int TRAILER_SIZE = 8;
        private static final boolean HAS_BROKEN_FINISH = JVM.isGZIPOutputStreamFinishBroken();
        private static final byte[] GZIP_HEADER = new byte[] {
            (byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED, 0,
            0, 0, 0, 0,
            0, (byte) 0xff
        };

        private final int level;
        private final CRC32 crc = new CRC32();

        private ResettableGzipStream(OutputStream out, int level) throws IOException {
            super(out,
                new Deflater(level <= 0 ? DEFAULT_LEVEL : level, true),
                DEFAULT_BUFFER_SIZE,
                false);
            this.level = level <= 0 ? DEFAULT_LEVEL : level;
            writeHeader();
            crc.reset();
        }

        private void resetState() throws IOException {
            def.reset();
            crc.reset();
            writeHeader();
        }

        @Override
        public synchronized void write(byte[] data, int offset, int length) throws IOException {
            super.write(data, offset, length);
            crc.update(data, offset, length);
        }

        @Override
        public synchronized void finish() throws IOException {
            if (def.finished()) {
                return;
            }
            try {
                def.finish();
                while (!def.finished()) {
                    int written = def.deflate(this.buf, 0, this.buf.length);
                    if (def.finished() && written <= this.buf.length - TRAILER_SIZE) {
                        writeTrailer(this.buf, written);
                        written += TRAILER_SIZE;
                        out.write(this.buf, 0, written);
                        return;
                    }
                    if (written > 0) {
                        out.write(this.buf, 0, written);
                    }
                }
                byte[] trailer = new byte[TRAILER_SIZE];
                writeTrailer(trailer, 0);
                out.write(trailer);
            } catch (IOException e) {
                if (HAS_BROKEN_FINISH) {
                    def.end();
                }
                throw e;
            }
        }

        private void writeHeader() throws IOException {
            out.write(GZIP_HEADER);
        }

        private void writeTrailer(byte[] buffer, int offset) {
            writeInt((int) crc.getValue(), buffer, offset);
            writeInt(def.getTotalIn(), buffer, offset + 4);
        }

        private void writeInt(int value, byte[] buffer, int offset) {
            writeShort(value & 0xFFFF, buffer, offset);
            writeShort((value >>> 16) & 0xFFFF, buffer, offset + 2);
        }

        private void writeShort(int value, byte[] buffer, int offset) {
            buffer[offset] = (byte) (value & 0xFF);
            buffer[offset + 1] = (byte) ((value >>> 8) & 0xFF);
        }
    }
}
