package work.impl;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import work.impl.InfoWritable;

/**
 * sorting by volume descending， inheritage the WritableComparator   year-month-volume
 */
public class SortComparator extends WritableComparator {

    /**
     * call the infowritable class
     */
    SortComparator() {
        super(InfoWritable.class, true);
    }


    /**
     * compare data,sort by descending
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        InfoWritable ia = (InfoWritable) a;
        InfoWritable ib = (InfoWritable) b;

        int result = Integer.compare(ia.getYear(), ib.getYear());
        if (result == 0) {
            result = Integer.compare(ia.getMonth(), ib.getMonth());
            if (result == 0) {
                return -Integer.compare(ia.getVolume(), ib.getVolume());
            } else {
                return result;
            }
        } else {
            return result;
        }
    }
}