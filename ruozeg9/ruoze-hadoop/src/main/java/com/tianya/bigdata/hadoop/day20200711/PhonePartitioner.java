package com.tianya.bigdata.hadoop.day20200711;

import com.tianya.bigdata.hadoop.day20200708.Access;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区是在map后面的 所以分区的类型
 */
public class PhonePartitioner extends Partitioner<Text, Access> {

    /**
     * Get the partition number for a given key (hence record) given the total
     * number of partitions i.e. number of reduce-tasks for the job.
     *
     * <p>Typically a hash function on a all or a subset of the key.</p>
     *
     * @param key          the key to be partioned.
     * @param value        the entry value.
     * @param numPartitions the total number of partitions.
     * @return the partition number for the <code>key</code>.
     */
    @Override
    public int getPartition(Text key, Access value, int numPartitions) {
        String phone = key.toString();
        if(phone.startsWith("13")){
            return 0;
        }else if(phone.startsWith("15")){
            return 1;
        }
        return 2;
    }
}
