package ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;

public class AdvancedApi {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        {// 分流
            FSDataInputStream in = fs.open(new Path("/ch3/a"));
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0);
            File file = new File("C:\\nos\\4-my-hadoop\\test\\split_file.txt");
            FileOutputStream out = new FileOutputStream(file);
            IOUtils.copyBytes(in, out, 4096, false);
            IOUtils.closeStream(in);
        }
        {// 过滤
            for (FileStatus p : fs.globStatus(new Path("/*/a"))) {
                System.out.println(p.getPath());
            }
            for (FileStatus p : fs.globStatus(new Path("/ch?"))) {
                System.out.println(p.getPath());
            }
        }
        fs.close();
    }
}
