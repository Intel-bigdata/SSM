package org.smartdata.hdfs.action.move;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;

public class DBlockStriped extends DBlock {

  final byte[] indices;
  final short dataBlockNum;
  final int cellSize;

  public DBlockStriped(Block block, byte[] indices, short dataBlockNum,
                       int cellSize) {
    super(block);
    this.indices = indices;
    this.dataBlockNum = dataBlockNum;
    this.cellSize = cellSize;
  }

  public DBlock getInternalBlock(StorageGroup storage) {
    int idxInLocs = locations.indexOf(storage);
    if (idxInLocs == -1) {
      return null;
    }
    byte idxInGroup = indices[idxInLocs];
    long blkId = getBlock().getBlockId() + idxInGroup;
    long numBytes = StripedBlockUtil.getInternalBlockLength(getNumBytes(), cellSize,
        dataBlockNum, idxInGroup);
    Block blk = new Block(getBlock());
    blk.setBlockId(blkId);
    blk.setNumBytes(numBytes);
    DBlock dblk = new DBlock(blk);
    dblk.addLocation(storage);
    return dblk;
  }
}
