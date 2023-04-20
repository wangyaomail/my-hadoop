package ch8;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZookeeperConnection {
    static CountDownLatch latch = new CountDownLatch(1);
    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper(
                "zzti:2181,zzti1:2181,zzti2:2181",
                300000,
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        System.out.println(event.getPath()
                                +","+event.getType()
                                +","+event.getState());
                        latch.countDown();
                    }
                }
        );
        latch.await();

        if(zk.exists("/a", false)!=null){
            ZKUtil.deleteRecursive(zk, "/a");
        }
        zk.create("/a", "a".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        for(int i=0;i<10;i++){
            zk.create("/a/"+i, ("a"+i).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }
        for( String c :zk.getChildren("/a",false)){
            System.out.println(c+ new String(zk.getData("/a/"+c,false,null)));
        }
        zk.close();
    }
}
