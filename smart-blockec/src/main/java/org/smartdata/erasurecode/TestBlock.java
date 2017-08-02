package org.smartdata.erasurecode;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by intel on 17-7-20.
 */
public class TestBlock extends ECBlock{

    public ECChunk[] chunks;
    private int numChunksInBlock;
    public Random RAND;

    TestBlock(int numChunksInBlock) {
        this.numChunksInBlock = numChunksInBlock;
        this.chunks = new ECChunk[numChunksInBlock];
        RAND = new Random();
    }

    TestBlock(int numChunksInBlock, ECChunk[] chunks) {
        this.numChunksInBlock = numChunksInBlock;
        this.chunks = chunks;
        RAND = new Random();
    }

    public void fillDummyData(int buflen) {
        for (int i = 0; i < numChunksInBlock; ++i) {
            byte[] dummy = new byte[buflen];
            RAND.nextBytes(dummy);
            ByteBuffer buffer = ByteBuffer.allocate(buflen);
            buffer.limit(buflen);
            buffer.put(dummy);
            buffer.flip();
            chunks[i] = new ECChunk(buffer);
        }
    }

    public void allocateBuffer(int buflen) {
        for (int i = 0; i < numChunksInBlock; ++i) {
            byte[] dummy = new byte[buflen];
            ByteBuffer buffer = ByteBuffer.allocate(buflen);
            buffer.limit(buflen);
            buffer.put(dummy);
            buffer.flip();
            chunks[i] = new ECChunk(buffer);
        }
        this.setParity(true);
    }

    public ECChunk[] getChunks() {
        return chunks;
    }
}
