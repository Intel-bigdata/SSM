package org.smartdata.erasurecode;
import org.apache.hadoop.conf.Configuration;
import org.smartdata.erasurecode.coder.*;
import org.smartdata.erasurecode.rawcoder.*;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Created by intel on 17-7-18.
 */
public class testBlockEC {
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

    enum CODER {
        DUMMY_CODER("Dummy coder"),
        XOR_CODER("XOR java coder"),
        RS_CODER("Reed-Solomon Java coder"),
        Native_XOR_CODER("Native ISA-L XOR coder"),
        Native_RS_CODER("Native ISA-L Reed-Solomon coder");
        private final String name;

        CODER(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public void setup(int numDataUnits, int numParityUnits,int chunkSize) {
        this.numDataUnits = numDataUnits;
        this.numParityUnits = numParityUnits;
        this.numChunksInBlock = 16;
        this.chunkSize = chunkSize;
        startBufferWithZero = true;

    }

    void createEncoder(int index) {
        try {
            ErasureCoderOptions options = new ErasureCoderOptions(
                    numDataUnits, numParityUnits, true, true);

            switch (index) {
                case 0:
                    encoderClass = DummyErasureEncoder.class;
                    break;
                case 1:
                    encoderClass = XORErasureEncoder.class;
                    break;
                case 2:
                    encoderClass = RSErasureEncoder.class;
                    break;
                case 3:
                    encoderClass = NativeXORErasureEncoder.class;
                    break;
                case 4:
                    encoderClass = NativeRSErasureEncoder.class;
                    break;
            }
            Constructor<? extends ErasureCoder> constructor =
                    (Constructor<? extends ErasureCoder>)
                            encoderClass.getConstructor(ErasureCoderOptions.class);
            encoder = constructor.newInstance(options);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create encoder", e);
        }
    }

    void createDecoder(int index) {
        try {
            ErasureCoderOptions options = new ErasureCoderOptions(
                    numDataUnits, numParityUnits, true, true);

            switch (index) {
                case 0:
                    encoderClass = DummyErasureDecoder.class;
                    break;
                case 1:
                    encoderClass = XORErasureDecoder.class;
                    break;
                case 2:
                    encoderClass = RSErasureDecoder.class;
                    break;
                case 3:
                    encoderClass = NativeXORErasureDecoder.class;
                    break;
                case 4:
                    encoderClass = NativeRSErasureDecoder.class;
                    break;
            }
            Constructor<? extends ErasureCoder> constructor =
                    (Constructor<? extends ErasureCoder>)
                            encoderClass.getConstructor(ErasureCoderOptions.class);
            decoder = constructor.newInstance(options);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create decoder", e);
        }
    }

    ECChunk allocateOutputChunk() {
        byte[] dummy = new byte[chunkSize];
        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        buffer.limit(chunkSize);
        buffer.put(dummy);
        buffer.flip();

        return new ECChunk(buffer);
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

        return new TestBlock(numChunksInBlock, newChunks);
    }

    protected ECBlockGroup EraseBlocks(ECBlockGroup blockGroup) {
        TestBlock[] erasedBlocks = (TestBlock[]) blockGroup.getDataBlocks();

        int eraseIndex = 3;

        for (int i = 0; i < numChunksInBlock; ++i) {
            erasedBlocks[eraseIndex].chunks[i] = null;
            erasedBlocks[eraseIndex].setErased(true);
        }

        blockGroup = new ECBlockGroup(erasedBlocks, blockGroup.getParityBlocks());
        return blockGroup;

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
        else {
            System.out.println("Compare and verify success");
        }
    }

    protected void compareAndVerify(ECBlock[] originBlocks,
                                    ECBlock[] recoveredBlocks) {
        for (int i = 0; i < originBlocks.length; ++i) {
            compareAndVerify(((TestBlock) originBlocks[i]).chunks, ((TestBlock) recoveredBlocks[i]).chunks);
        }
    }

    private ECBlockGroup fillData(int numDataUnits,int numParityUnits) {


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

        ECBlockGroup blockGroup = new ECBlockGroup(dataBlocks, parityBlocks);
        return blockGroup;

    }

    public void doEncode( ECBlockGroup blockGroup) {


        ErasureCodingStep codingStep = encoder.calculateCoding(blockGroup);

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

    public void doDecode( ECBlockGroup blockGroup) {

        ErasureCodingStep codingStep = decoder.calculateCoding(blockGroup);
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

    private static void printAvailableCoders() {
        StringBuilder sb = new StringBuilder(
                "Available coders with coderIndex:\n");
        for (CODER coder : CODER.values()) {
            sb.append(coder.ordinal()).append(":").append(coder).append("\n");
        }
        System.out.println(sb.toString());
    }

    private static void usage(String message) {
        if (message != null) {
            System.out.println(message);
        }
        System.out.println(
                "Usage: testBlockEC  <coderIndex> " +
                        "<numDataUnits> <numParityUnits> [chunkSize]");
        printAvailableCoders();
        System.exit(1);
    }

    private boolean checkECSchema() {
        return true;
    }

    private void run(int coderIndex,int numDataUnits, int numParityUnits,int chunkSize) {
        setup(numDataUnits, numParityUnits,chunkSize);

        ECBlockGroup blockGroup = fillData(numDataUnits,numParityUnits);

        TestBlock[] backup = cloneBlocksWithData((TestBlock[])blockGroup.getDataBlocks());

        CODER coder = CODER.values()[coderIndex];

        createEncoder(coder.ordinal());
        createDecoder(coder.ordinal());

        doEncode(blockGroup);

        blockGroup = new ECBlockGroup(backup,blockGroup.getParityBlocks());

        blockGroup = EraseBlocks(blockGroup);

        doDecode(blockGroup);

        compareAndVerify(backup,blockGroup.getDataBlocks());
    }
    public static void main(String args[]) {
        testBlockEC testBlockEC = new testBlockEC();
        String opType = null;
        int coderIndex = 0;
        int numDataUnits = 6;
        int numParityUnits = 1;
        int chunkSize = 1024;
        if (args.length > 0) {
            try {
                coderIndex = Integer.parseInt(args[0]);
                if (coderIndex < 0 || coderIndex > CODER.values().length) {
                    usage("Invalid coder index, should be [0-" +
                            (CODER.values().length - 1) + "]");
                }
            } catch (NumberFormatException e) {
                usage("Malformed coder index, " + e.getMessage());
            }
        } else {
            usage(null);
        }
        if (args.length > 1) {
            try {
                numDataUnits = Integer.parseInt(args[1]);
                if (numDataUnits < 0) {
                    usage("Invalid numDataUnits");
                }
            } catch (NumberFormatException e) {
                usage("Malformed numDataUnits");
            }
        }
        if (args.length > 2) {
            try {
                numParityUnits = Integer.parseInt(args[2]);
                if (numParityUnits < 0) {
                    usage("Invalid numParityUnits");
                }
            } catch (NumberFormatException e) {
                usage("Malformed numParityUnits");
            }
        }

        if (args.length > 3) {
            try {
                chunkSize = Integer.parseInt(args[3]);
                if (chunkSize < 0) {
                    usage("Invalid numParityUnits");
                }
            } catch (NumberFormatException e) {
                usage("Malformed numParityUnits");
            }
        }

        testBlockEC.run(coderIndex,numDataUnits,numParityUnits,chunkSize);
    }
}
