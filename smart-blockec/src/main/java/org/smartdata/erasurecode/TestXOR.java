package org.smartdata.erasurecode;
import com.sun.xml.internal.stream.util.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.smartdata.erasurecode.coder.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by intel on 17-7-18.
 */
public class TestXOR {
    protected Class<? extends ErasureCoder> encoderClass;
    protected Class<? extends ErasureCoder> decoderClass;

    private ErasureCoder encoder;
    private ErasureCoder decoder;

    private int numDataUnits;
    private int numParityUnits;
    private int numChunksInBlock;
    private int chunkSize;
    private boolean startBufferWithZero;
    private Configuration conf;

    private TestBlock testBlock;
    protected static Random RAND = new Random();

    public void setup() {
        encoderClass = XORErasureEncoder.class;
        decoderClass = XORErasureDecoder.class;

        numDataUnits = 10;
        numParityUnits = 1;
        numChunksInBlock = 16;
        chunkSize = 1024;
        startBufferWithZero = true;

        conf = new Configuration();

    }

    void prepareCoder() {
        ErasureCoderOptions options = new ErasureCoderOptions(
                numDataUnits, numParityUnits, true, true);

        encoder = new XORErasureEncoder(options);
        encoder.setConf(conf);
        decoder = new XORErasureDecoder(options);
        decoder.setConf(conf);
    }

    ECChunk allocateOutputChunk() {
        byte[] dummy = new byte[chunkSize];
        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        buffer.limit(chunkSize);
        buffer.put(dummy);
        buffer.flip();

        return new ECChunk(buffer);
    }
    void encode(ErasureCodingStep codingStep) {
        // Pretend that we're opening these input blocks and output blocks.
        ECBlock[] inputBlocks = codingStep.getInputBlocks();
        ECBlock[] outputBlocks = codingStep.getOutputBlocks();
        // We allocate input and output chunks accordingly.
        ECChunk[] inputChunks = new ECChunk[inputBlocks.length];
        ECChunk[] outputChunks = new ECChunk[outputBlocks.length];

        for (int i = 0; i < numChunksInBlock; ++i) {
            // Pretend that we're reading input chunks from input blocks.
            for (int j = 0; j < inputBlocks.length; ++j) {
                inputChunks[j] = ((TestBlock) inputBlocks[j]).chunks[i];
            }

            // Pretend that we allocate and will write output results to the blocks.
            for (int j = 0; j < outputBlocks.length; ++j) {
                outputChunks[j] = allocateOutputChunk();
                ((TestBlock) outputBlocks[j]).chunks[i] = outputChunks[j];
            }

            // Given the input chunks and output chunk buffers, just call it !
            codingStep.performCoding(inputChunks, outputChunks);
        }

        codingStep.finish();
    }

    void decode(ErasureCodingStep codingStep) {
        // Pretend that we're opening these input blocks and output blocks.
        ECBlock[] inputBlocks = codingStep.getInputBlocks();
        ECBlock[] outputBlocks = codingStep.getOutputBlocks();
        // We allocate input and output chunks accordingly.
        ECChunk[] inputChunks = new ECChunk[inputBlocks.length];
        ECChunk[] outputChunks = new ECChunk[outputBlocks.length];

        for (int i = 0; i < numChunksInBlock; ++i) {
            // Pretend that we're reading input chunks from input blocks.
            for (int j = 0; j < inputBlocks.length; ++j) {
                inputChunks[j] = ((TestBlock) inputBlocks[j]).chunks[i];
            }

            // Pretend that we allocate and will write output results to the blocks.
            for (int j = 0; j < outputBlocks.length; ++j) {
                outputChunks[j] = allocateOutputChunk();
                ((TestBlock) outputBlocks[j]).chunks[i] = outputChunks[j];
            }

            // Given the input chunks and output chunk buffers, just call it !
            codingStep.performCoding(inputChunks, outputChunks);
        }

        codingStep.finish();
    }

    protected ECChunk[] cloneChunksWithData(ECChunk[] chunks) {
        ECChunk[] results = new ECChunk[chunks.length];
        for (int i = 0; i < chunks.length; i++) {
            results[i] = cloneChunkWithData(chunks[i]);
        }

        return results;
    }

    protected ByteBuffer allocateOutputBuffer(int bufferLen) {
        /**
         * When startBufferWithZero, will prepare a buffer as:---------------
         * otherwise, the buffer will be like:             ___TO--BE--WRITTEN___,
         * and in the beginning, dummy data are prefixed, to simulate a buffer of
         * position > 0.
         */
        int startOffset = startBufferWithZero ? 0 : 11; // 11 is arbitrary
        int allocLen = startOffset + bufferLen + startOffset;
        ByteBuffer buffer = ByteBuffer.allocate(allocLen);
        buffer.limit(startOffset + bufferLen);
        //startBufferWithZero = ! startBufferWithZero;

        return buffer;
    }
    protected ECChunk cloneChunkWithData(ECChunk chunk) {
        if (chunk == null) {
            return null;
        }

        ByteBuffer srcBuffer = chunk.getBuffer();

        byte[] bytesArr = new byte[srcBuffer.remaining()];

        srcBuffer.mark();
        srcBuffer.get(bytesArr, 0, bytesArr.length);
        srcBuffer.reset();

        ByteBuffer destBuffer = allocateOutputBuffer(bytesArr.length);
        int pos = destBuffer.position();
        destBuffer.put(bytesArr);
        destBuffer.flip();
        destBuffer.position(pos);

        return new ECChunk(destBuffer);
    }

    protected TestBlock[] cloneBlocksWithData(TestBlock[] blocks) {
        TestBlock[] results = new TestBlock[blocks.length];
        for (int i = 0; i < blocks.length; ++i) {
            results[i] = cloneBlockWithData(blocks[i]);
        }

        return results;
    }


    protected TestBlock cloneBlockWithData(TestBlock block) {
        ECChunk[] newChunks = cloneChunksWithData(block.getChunks());

        return new TestBlock(numChunksInBlock,newChunks);
    }

    protected void EraseBlocks(TestBlock[] dataBlocks) {
        int eraseIndex = 0;

        for(int i = 0; i < numChunksInBlock; ++i){
            dataBlocks[eraseIndex].chunks[i] = null;
            dataBlocks[eraseIndex].setErased(true);
        }
    }

    protected byte[][] toArrays(ECChunk[] chunks) {
        byte[][] bytesArr = new byte[chunks.length][];

        for (int i = 0; i < chunks.length; i++) {
            if (chunks[i] != null) {
                chunks[i].getBuffer().position(0);
                bytesArr[i] = chunks[i].toBytesArray();
            }
        }

        return bytesArr;
    }

    protected void compareAndVerify(ECChunk[] erasedChunks,
                                    ECChunk[] recoveredChunks) {
        byte[][] erased = toArrays(erasedChunks);
        byte[][] recovered = toArrays(recoveredChunks);
        boolean result = Arrays.deepEquals(erased, recovered);
        if (!result) {
            System.out.println("Decoding and comparing failed./n");
        }
    }

    protected void compareAndVerify(ECBlock[] erasedBlocks,
                                    ECBlock[] recoveredBlocks) {
        for (int i = 0; i < erasedBlocks.length; ++i) {
            compareAndVerify(((TestBlock) erasedBlocks[i]).chunks, ((TestBlock) recoveredBlocks[i]).chunks);
        }
    }

    public void run() {


        // preapare dataBlocks and Parity Block
        // for simply purpose, do not use the hdfs block
        setup();

        TestBlock[] dataBlocks = new TestBlock[numDataUnits];
        TestBlock[] parityBlocks = new TestBlock[numParityUnits];

        for (int i = 0; i < numDataUnits; ++i) {
            dataBlocks[i] = new TestBlock(numChunksInBlock);
            dataBlocks[i].fillDummyData(chunkSize);
        }
        for (int i = 0; i < numParityUnits; ++i) {
            parityBlocks[i] = new TestBlock(numChunksInBlock);
            parityBlocks[i].allocateBuffer(chunkSize);
        }


        ECBlockGroup blockGroup = new ECBlockGroup(dataBlocks,parityBlocks);

        TestBlock[] clonedDataBlocks =
                cloneBlocksWithData((TestBlock[]) blockGroup.getDataBlocks());

        // prepare coder
        prepareCoder();

        // encode every chunks
        ErasureCodingStep codingStep = encoder.calculateCoding(blockGroup);

        TestBlock[] backup = cloneBlocksWithData((TestBlock[])blockGroup.getDataBlocks());

        encode(codingStep);

        // recover dataBlocks
        EraseBlocks(clonedDataBlocks);

        blockGroup = new ECBlockGroup(clonedDataBlocks, blockGroup.getParityBlocks());

        codingStep = decoder.calculateCoding(blockGroup);
        decode(codingStep);

        // compare

        compareAndVerify(backup,blockGroup.getDataBlocks());

    }
    public static void main(String args[]){
        TestXOR testXOR = new TestXOR();
        testXOR.run();
    }
}
