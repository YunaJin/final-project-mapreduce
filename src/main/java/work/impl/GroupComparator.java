package work.impl;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *  create the class, put same year into groups
 */
public class GroupComparator extends WritableComparator {

    GroupComparator() {
        super(InfoWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        InfoWritable ia = (InfoWritable) a;
        InfoWritable ib = (InfoWritable) b;

        int result = Integer.compare(ia.getYear(), ib.getYear());
        if (result == 0) {
            return Integer.compare(ia.getMonth(), ib.getMonth());
        } else
            return result;
    }
}