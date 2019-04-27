package work.impl;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import work.impl.InfoWritable;

/**
 * 排序类，继承WritableComparator   年-月-成交量  按照成交量降序
 */
public class SortComparator extends WritableComparator {

    /**
     * 调用父类的构造函数
     */
    SortComparator() {
        super(InfoWritable.class, true);
    }


    /**
     * 比较两个对象的大小，使用降序排列
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