package ch5;

/**
 * 在yarn申请服务后执行的简单小程序
 */
public class ch502simpleAppMaster {
    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            try {
                Thread.sleep(1000);
                System.out.println(i + ":" + System.currentTimeMillis());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
