package work.impl;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoWritable implements WritableComparable<InfoWritable> {

    private int year;
    private int month;
    private int day;
    //trading volume
    private int volume;

    @Override
    public int compareTo(InfoWritable o) {

        int result = Integer.compare(this.getYear(), o.getYear());
        if (result == 0) {
            result = Integer.compare(this.getMonth(), o.getMonth());
            if (result == 0) {
                return -Double.compare(this.getVolume(), o.getVolume());
            } else
                return result;
        } else
            return result;

        //return this==o?0:-1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.month);
        dataOutput.writeInt(this.day);
        dataOutput.writeDouble(this.volume);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.month = dataInput.readInt();
        this.day = dataInput.readInt();
        this.volume = dataInput.readInt();
    }


    public void setYear(int year) {
        this.year = year;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public void setVolume(int volume) {
        this.volume = volume;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getDay() {
        return day;
    }

    public int getVolume() {
        return volume;
    }

}