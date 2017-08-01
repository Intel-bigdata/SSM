package org.smartdata.erasurecode.coder;

import org.smartdata.erasurecode.*;
import org.smartdata.erasurecode.rawcoder.NativeXORRawEncoder;
import org.smartdata.erasurecode.rawcoder.RawErasureDecoder;
import org.smartdata.erasurecode.rawcoder.RawErasureEncoder;

/**
 * Created by intel on 17-7-31.
 */
public class NativeXORErasureEncoder extends ErasureEncoder {
    private RawErasureEncoder rawEncoder;

    public NativeXORErasureEncoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareEncodingStep(
            final ECBlockGroup blockGroup) {
        RawErasureEncoder rawEncoder = new NativeXORRawEncoder(getOptions());

        ECBlock[] inputBlocks = getInputBlocks(blockGroup);

        return new ErasureEncodingStep(inputBlocks,
                getOutputBlocks(blockGroup), rawEncoder);
    }

}
