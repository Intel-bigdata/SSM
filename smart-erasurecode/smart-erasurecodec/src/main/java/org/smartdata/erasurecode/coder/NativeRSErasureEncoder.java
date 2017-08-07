package org.smartdata.erasurecode.coder;

import org.smartdata.erasurecode.*;
import org.smartdata.erasurecode.rawcoder.NativeRSRawEncoder;
import org.smartdata.erasurecode.rawcoder.RawErasureEncoder;

/**
 * Created by intel on 17-7-31.
 */
public class NativeRSErasureEncoder extends ErasureEncoder {
    private RawErasureEncoder rawEncoder;

    public NativeRSErasureEncoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareEncodingStep(final ECBlockGroup blockGroup) {

        RawErasureEncoder rawEncoder = checkCreateRSRawEncoder();

        ECBlock[] inputBlocks = getInputBlocks(blockGroup);

        return new ErasureEncodingStep(inputBlocks,
                getOutputBlocks(blockGroup), rawEncoder);
    }

    private RawErasureEncoder checkCreateRSRawEncoder() {
        if (rawEncoder == null) {
            // TODO: we should create the raw coder according to codec.
            rawEncoder = new NativeRSRawEncoder(getOptions());
        }
        return rawEncoder;
    }

    @Override
    public void release() {
        if (rawEncoder != null) {
            rawEncoder.release();
        }
    }

    @Override
    public boolean preferDirectBuffer() {
        return false;
    }
}
