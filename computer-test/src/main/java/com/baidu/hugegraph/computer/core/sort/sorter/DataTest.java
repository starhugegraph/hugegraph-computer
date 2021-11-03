package com.baidu.hugegraph.computer.core.sort.sorter;

import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.DefaultKvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class DataTest {

    @Test
    public void dataTest() throws Exception {
        final int size = 20000000;
        Random random = new Random();
        StopWatch watch = new StopWatch();

        watch.start();
        KvEntry[] data = new DefaultKvEntry[size];
        for (int i = 0; i < size; i++) {
            UnsafeBytesOutput output = new UnsafeBytesOutput(16);
            output.writeInt(Integer.BYTES * 3);
            output.writeInt(random.nextInt(size));
            output.writeInt(random.nextInt(size));
            output.writeInt(random.nextInt(size));
            output.writeInt(Integer.BYTES);
            output.writeInt(random.nextInt(size));
            KvEntry kvEntry = EntriesUtil.kvEntryFromInput(
                              new UnsafeBytesInput(
                                  output.buffer(), (int) output.position()),
                                  true, false);
            data[i] = kvEntry;
        }
        watch.stop();
        System.out.println("create data spend time: " + watch.getTime());

        watch.reset();
        watch.start();
        Arrays.sort(data);
        watch.stop();

        System.out.println("sort data spend time: " + watch.getTime());
    }
}
