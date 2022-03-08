package ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;

public class BasicApi {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        {// 清空目录/test
            fs.delete(new Path("/ch3"), true);
        }
        {// 创建文件夹/ch4
            fs.mkdirs(new Path("/ch3"));
        }
        {// 上传并覆盖文件
            Path from = new Path("C:\\nos\\4-my-hadoop\\data\\the_old_man_and_sea.txt");
            for (String name : new String[] { "a", "b", "c", "d" }) {
                Path to = new Path("/ch3/" + name);
                fs.copyFromLocalFile(from, to);
            }
        }
        {// 上传并追加集群文件
            Path to = new Path("/ch3/a");
            Path[] from = { new Path("/ch3/b"), new Path("/ch3/c"), new Path("/ch3/d") };
            fs.concat(to, from);
        }
        {// 打印文件
            //            InputStream in = fs.open(new Path("/ch3/a"));
            //            IOUtils.copyBytes(in, System.out, 4096, false);
            //            IOUtils.closeStream(in);
        }
        {// 打印目录
            for (FileStatus p : fs.listStatus(new Path("/"))) {
                System.out.println(p.getPath());
            }
        }
        System.out.println("123");
        {// 循环打印目录
            RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path("/"), true);
            while (itr.hasNext()) {
                LocatedFileStatus fstatus = itr.next();
                String pwd = fstatus.getPath().getParent() + "/" + fstatus.getPath().getName();
                System.out.println(pwd);
            }
        }
        {// 下载文件到本地目录
            Path from = new Path("/ch3/a");
            Path to = new Path("C:\\nos\\4-my-hadoop\\test\\");
            fs.copyToLocalFile(from, to);
        }
        {// 打印文件的状态
            FileStatus fstatus = fs.getFileStatus(new Path("/ch3/a"));
            System.out.println("文件名" + fstatus.getPath().getName());
            System.out.println("文件路径" + fstatus.getPath().getParent());
            System.out.println("大小" + fstatus.getLen());
            System.out.println("block大小" + fstatus.getBlockSize());
            System.out.println("修改时间(ms)" + fstatus.getModificationTime());
        }

        fs.close();
    }
}
