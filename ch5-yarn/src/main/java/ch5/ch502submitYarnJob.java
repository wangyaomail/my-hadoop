package ch5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 提交一个简单任务
 */
public class ch502submitYarnJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new YarnConfiguration();
        // 先上传jar到DFS
        FileSystem fs = FileSystem.get(conf);
        String localProjectPath = new File("").getAbsolutePath();
        Path from = new Path(localProjectPath + "/ch5-yarn/target/ch5-yarn-1.0.jar");
        Path to = new Path("/ch5");
        if (fs.exists(to)) {
            fs.delete(new Path("/ch5"), true);
            fs.mkdirs(new Path("/ch5"));
        }
        fs.copyFromLocalFile(from, to);

        // 启动yarn
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        YarnClientApplication app = yarnClient.createApplication();

        ContainerLaunchContext appMasterContainer = Records.newRecord(ContainerLaunchContext.class);
        StringBuilder sb = new StringBuilder();
        sb.append("$JAVA_HOME/bin/java");
        sb.append(" -jar");
        sb.append(" ch5-yarn-1.0.jar");
        sb.append(" ch5.ch502simpleAppMaster");

        appMasterContainer.setCommands(Collections.singletonList(sb.toString()));

        LocalResource appMasterJar = Records.newRecord(LocalResource.class);

        Path jarPath = to;

        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
        appMasterContainer.setLocalResources(Collections.singletonMap("ch5-yarn-1.0.jar",
                                                                      appMasterJar));

        Map<String, String> appMasterEnv = new HashMap<>();
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv,
                                  ApplicationConstants.Environment.CLASSPATH.name(),
                                  c.trim());
        }
        Apps.addToEnvironment(appMasterEnv,
                              ApplicationConstants.Environment.CLASSPATH.name(),
                              ApplicationConstants.Environment.PWD.$() + File.separator + "*");
        appMasterContainer.setEnvironment(appMasterEnv);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemorySize(256);
        capability.setVirtualCores(1);

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName("yarn test app");
        appContext.setAMContainerSpec(appMasterContainer);
        appContext.setResource(capability);
        appContext.setQueue("default");

        System.out.println(appContext.getApplicationId());
        System.out.println(appContext.getResource());

        yarnClient.submitApplication(appContext);
        //        yarnClient.close();
    }
}
