package alien.io;

import static java.lang.Long.rotateLeft;

/*
 * Copyright 2020 Linnaea Von Lavia and the lz4-java contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Link to original code source
 * https://github.com/lz4/lz4-java/blob/master/src/java/net/jpountz/xxhash/StreamingXXHash64.java
*/


/**
 * @author lz4-java team
 */
public class StreamingXXHash64 {
    private int memSize;
    private long v1, v2, v3, v4;
    private long totalLen;
    private final byte[] memory;
    private long seed;

    private final long PRIME1 = -7046029288634856825L; //11400714785074694791
    private final long PRIME2 = -4417276706812531889L; //14029467366897019727
    private final long PRIME3 = 1609587929392839161L;
    private final long PRIME4 = -8796714831421723037L; //9650029242287828579
    private final long PRIME5 = 2870177450012600261L;

    /**
     * @param seed
     */
    public StreamingXXHash64(long seed) {
        totalLen = 0;
        memSize = 0;
        memory = new byte[32];
        v1 = seed + PRIME1 + PRIME2;
        v2 = seed + PRIME2;
        v3 = seed;
        v4 = seed - PRIME1;
        this.seed = seed;
    }

    private static long readLongLE(byte[] buf, int i) {
        return (buf[i] & 0xFFL) | ((buf[i+1] & 0xFFL) << 8) | ((buf[i+2] & 0xFFL) << 16) | ((buf[i+3] & 0xFFL) << 24)
                | ((buf[i+4] & 0xFFL) << 32) | ((buf[i+5] & 0xFFL) << 40) | ((buf[i+6] & 0xFFL) << 48) | ((buf[i+7] & 0xFFL) << 56);
    }

    private static int readIntLE(byte[] buf, int i) {
        return (buf[i] & 0xFF) | ((buf[i+1] & 0xFF) << 8) | ((buf[i+2] & 0xFF) << 16) | ((buf[i+3] & 0xFF) << 24);
    }

    /**
     * @param buf
     * @param offset
     * @param len
     */
    public void update(final byte[] buf, final int offset, final int len) {
        if (offset < 0 || offset >= buf.length) {
            throw new ArrayIndexOutOfBoundsException(offset);
        }
        
        int off = offset;

        totalLen += len;

        if (memSize + len < 32) { // fill in tmp buffer
            System.arraycopy(buf, off, memory, memSize, len);
            memSize += len;
            return;
        }

        final int end = off + len;

        if (memSize > 0) { // data left from previous update
            System.arraycopy(buf, off, memory, memSize, 32 - memSize);

            v1 += readLongLE(memory, 0) * PRIME2;
            v1 = rotateLeft(v1, 31);
            v1 *= PRIME1;

            v2 += readLongLE(memory, 8) * PRIME2;
            v2 = rotateLeft(v2, 31);
            v2 *= PRIME1;

            v3 += readLongLE(memory, 16) * PRIME2;
            v3 = rotateLeft(v3, 31);
            v3 *= PRIME1;

            v4 += readLongLE(memory, 24) * PRIME2;
            v4 = rotateLeft(v4, 31);
            v4 *= PRIME1;

            off += 32 - memSize;
            memSize = 0;
        }

        {
            final int limit = end - 32;
            long lv1 = this.v1;
            long lv2 = this.v2;
            long lv3 = this.v3;
            long lv4 = this.v4;

            while (off <= limit) {
                lv1 += readLongLE(buf, off) * PRIME2;
                lv1 = rotateLeft(lv1, 31);
                lv1 *= PRIME1;
                off += 8;

                lv2 += readLongLE(buf, off) * PRIME2;
                lv2 = rotateLeft(lv2, 31);
                lv2 *= PRIME1;
                off += 8;

                lv3 += readLongLE(buf, off) * PRIME2;
                lv3 = rotateLeft(lv3, 31);
                lv3 *= PRIME1;
                off += 8;

                lv4 += readLongLE(buf, off) * PRIME2;
                lv4 = rotateLeft(lv4, 31);
                lv4 *= PRIME1;
                off += 8;
            }

            this.v1 = lv1;
            this.v2 = lv2;
            this.v3 = lv3;
            this.v4 = lv4;
        }

        if (off < end) {
            System.arraycopy(buf, off, memory, 0, end - off);
            memSize = end - off;
        }
    }

    /**
     * @return hash value
     */
    public long getValue() {
        long h64;
        if (totalLen >= 32) {
            long lv1 = this.v1;
            long lv2 = this.v2;
            long lv3 = this.v3;
            long lv4 = this.v4;

            h64 = rotateLeft(lv1, 1) + rotateLeft(lv2, 7) + rotateLeft(lv3, 12) + rotateLeft(lv4, 18);

            lv1 *= PRIME2; lv1 = rotateLeft(lv1, 31); lv1 *= PRIME1; h64 ^= lv1;
            h64 = h64* PRIME1 + PRIME4;

            lv2 *= PRIME2; lv2 = rotateLeft(lv2, 31); lv2 *= PRIME1; h64 ^= lv2;
            h64 = h64* PRIME1 + PRIME4;

            lv3 *= PRIME2; lv3 = rotateLeft(lv3, 31); lv3 *= PRIME1; h64 ^= lv3;
            h64 = h64* PRIME1 + PRIME4;

            lv4 *= PRIME2; lv4 = rotateLeft(lv4, 31); lv4 *= PRIME1; h64 ^= lv4;
            h64 = h64* PRIME1 + PRIME4;
        } else {
            h64 = seed + PRIME5;
        }

        h64 += totalLen;

        int off = 0;
        while (off <= memSize - 8) {
            long k1 = readLongLE(memory, off);
            k1 *= PRIME2; k1 = rotateLeft(k1, 31); k1 *= PRIME1; h64 ^= k1;
            h64 = rotateLeft(h64, 27) * PRIME1 + PRIME4;
            off += 8;
        }

        if (off <= memSize - 4) {
            h64 ^= (readIntLE(memory, off) & 0xFFFFFFFFL) * PRIME1;
            h64 = rotateLeft(h64, 23) * PRIME2 + PRIME3;
            off += 4;
        }

        while (off < memSize) {
            h64 ^= (memory[off] & 0xFF) * PRIME5;
            h64 = rotateLeft(h64, 11) * PRIME1;
            ++off;
        }

        h64 ^= h64 >>> 33;
        h64 *= PRIME2;
        h64 ^= h64 >>> 29;
        h64 *= PRIME3;
        h64 ^= h64 >>> 32;

        return h64;
    }

}
