package c2023;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.InputStream;
import java.net.URI;

public class Hadoop3403013 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(URI.create("hdfs://zzti:9000"), conf, "zzti");
//        fs.mkdirs(new Path("/3333"));
        InputStream in = fs.open(new Path("/ch3/a"));
//        IOUtils.copyBytes(in, System.out, 4096, false);
//        IOUtils.closeStream(in);

//        fs.concat(new Path("/ch3/a"),new Path[]{
//                new Path("/ch3/b"),
//                new Path("/ch3/c"),
//                new Path("/ch3/d"),
//        });
//        for(FileStatus status: fs.listStatus(new Path("/"))){
//            System.out.println(status.getPath());
//        }
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path("/"), true);
        while(itr.hasNext()){
            LocatedFileStatus status = itr.next();
            System.out.println(status.getPath());
        }
        fs.delete(new Path("/ch3/a"));
        fs.delete(new Path("/ch3"), true);

        fs.close();
    }
}
