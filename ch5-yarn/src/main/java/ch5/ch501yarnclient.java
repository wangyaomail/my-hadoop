package ch5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 打开yarn客户端
 */
public class ch501yarnclient {

    public static void main(String[] args) throws Exception {
        Configuration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        System.out.println(appResponse.getApplicationId());
        System.out.println(appResponse.getMaximumResourceCapability());
        yarnClient.close();
    }
}
