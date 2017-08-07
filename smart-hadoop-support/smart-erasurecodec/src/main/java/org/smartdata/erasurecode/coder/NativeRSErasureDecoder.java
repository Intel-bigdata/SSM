package org.smartdata.erasurecode.coder;

import org.smartdata.erasurecode.*;
import org.smartdata.erasurecode.rawcoder.NativeRSRawDecoder;
import org.smartdata.erasurecode.rawcoder.RawErasureDecoder;

/**
 * Created by intel on 17-7-31.
 */
public class NativeRSErasureDecoder extends ErasureDecoder {
    private RawErasureDecoder rsRawDecoder;

    public NativeRSErasureDecoder(ErasureCoderOptions options) {
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
            rsRawDecoder = new NativeRSRawDecoder(getOptions());
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
