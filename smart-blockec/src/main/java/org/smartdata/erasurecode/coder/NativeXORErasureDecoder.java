package org.smartdata.erasurecode.coder;

import org.smartdata.erasurecode.*;
import org.smartdata.erasurecode.rawcoder.NativeXORRawDecoder;
import org.smartdata.erasurecode.rawcoder.RawErasureDecoder;

/**
 * Created by intel on 17-7-31.
 */
public class NativeXORErasureDecoder extends ErasureDecoder{
    private RawErasureDecoder rsRawDecoder;

    public NativeXORErasureDecoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareDecodingStep(final ECBlockGroup blockGroup) {

        ECBlock[] inputBlocks = getInputBlocks(blockGroup);
        ECBlock[] outputBlocks = getOutputBlocks(blockGroup);

        RawErasureDecoder rawDecoder = checkCreateRSRawDecoder();
        return new ErasureDecodingStep(inputBlocks,
                getErasedIndexes(inputBlocks), outputBlocks, rawDecoder);
    }

    private RawErasureDecoder checkCreateRSRawDecoder() {
        if (rsRawDecoder == null) {
            rsRawDecoder = new NativeXORRawDecoder(getOptions());
        }
        return rsRawDecoder;
    }

    @Override
    public void release() {
        if (rsRawDecoder != null) {
            rsRawDecoder.release();
        }
    }
}
