package com.tianya.bigdata.hadoop.day20200712.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区是在map后面的 所以分区的类型
 */
public class PhoneSortPartitioner extends Partitioner<Traffic,Text> {


    /**
     * Get the partition number for a given key (hence record) given the total
     * number of partitions i.e. number of reduce-tasks for the job.
     *
     * <p>Typically a hash function on a all or a subset of the key.</p>
     *
     * @param traffic       the key to be partioned.
     * @param text          the entry value.
     * @param numPartitions the total number of partitions.
     * @return the partition number for the <code>key</code>.
     */
    @Override
    public int getPartition(Traffic traffic, Text text, int numPartitions) {
        String phone = text.toString();
        if(phone.startsWith("13")){
            return 0;
        }else if(phone.startsWith("15")){
            return 1;
        }
        return 2;
    }
}
