package c2023;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;

public class Hadoop0313 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://zzti:9000"), conf, "zzti");
//        Path path = new Path("C:\\share\\hadoop\\code\\the_old_man_and_sea.txt");
//        for(String name : new String[]{"a","b","c","d"}){
//            fs.copyFromLocalFile(path, new Path("/ch3/"+name));
//        }
//        Path to = new Path("/ch3/a");
//        Path[] from = {new Path("/ch3/b"),
//                new Path("/ch3/c"),
//                new Path("/ch3/d")};
//        fs.concat(to, from);
//        for (FileStatus status : fs.listStatus(new Path("/"))) {
//            System.out.println(status.getPath().getName()
//            );
//        }
//        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path("/"), true);
//        while(itr.hasNext()){
//            LocatedFileStatus status = itr.next();
//            System.out.println(status.getPath());
//        }
//        fs.delete(new Path("/ch3"), true);
        FSDataInputStream in = fs.open(new Path("/ch3/a"));
        IOUtils.copyBytes(in, System.out, 4096, false);
        in.seek(0);
        File file = new File("C:\\share\\hadoop\\code\\split_file.txt");
        FileOutputStream out = new FileOutputStream(file);
        IOUtils.copyBytes(in, out, 4096, false);
        IOUtils.closeStream(in);

        fs.close();
    }
}
