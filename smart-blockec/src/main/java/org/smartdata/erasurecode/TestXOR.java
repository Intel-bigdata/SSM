package org.smartdata.erasurecode;
import org.smartdata.erasurecode.coder.*;

import java.nio.ByteBuffer;

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

    public void setup() {
        this.encoderClass = XORErasureEncoder.class;
        this.decoderClass = XORErasureDecoder.class;

        this.numDataUnits = 10;
        this.numParityUnits = 1;
        this.numChunksInBlock = 16;
    }

    public void prepareCoders() {

    }

    protected ByteBuffer allocateOutputBuffer(int bufferLen) {
        /**
         * When startBufferWithZero, will prepare a buffer as:---------------
         * otherwise, the buffer will be like:             ___TO--BE--WRITTEN___,
         * and in the beginning, dummy data are prefixed, to simulate a buffer of
         * position > 0.
         */
        int startOffset = startBufferWithZero ? 0 : 11; // 11 is arbitrary
        // int allocLen = startOffset + bufferLen + startOffset;
        // ByteBuffer buffer = allocator.allocate(allocLen);
        // buffer.limit(startOffset + bufferLen);
        // fillDummyData(buffer, startOffset);
        // startBufferWithZero = ! startBufferWithZero;

        return buffer;
    }

    public ECBlock generateChunks(ECBlock Block) {
        Block =
    }

    public void generateBlocks(ECBlock[] Blocks,boolean isParity) {
        Blocks = new ECBlock[numDataUnits];

        for(int i = 0; i < numDataUnits; i++){
            Blocks[i] = new ECBlock(isParity,false);
        }

    }
    public void main() {
        // step 1
        // generate test block
        // using direct buffer

        setup();

        ECBlock[] DataBlocks = null;
        ECBlock[] ParityBlocks = null;

        generateBlocks(DataBlocks,false);
        generateBlocks(ParityBlocks,true);

        ECBlockGroup ECBlockGroup = new ECBlockGroup(DataBlocks,ParityBlocks);

        ErasureCodingStep codingStep;


        // step 2
        // prepare XOR encoder/decoder
        prepareCoders();


        codingStep = encoder.calculateCoding(blockGroup);
        performCodingStep(codingStep);
        // Erase specified sources but return copies of them for later comparing
        TestBlock[] backupBlocks = backupAndEraseBlocks(clonedDataBlocks, parityBlocks);

        // step 3
        // encode/decode

        // step 4
        // compare backupBlock and outputBlock
    }
}
