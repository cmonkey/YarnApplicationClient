package com.trader.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class YarnApplicationClient {

    private static final Log log = LogFactory.getLog(YarnApplicationClient.class);

    private final Configuration configuration;
    private final YarnClient yarnClient;
    private final String appMasterMainClass;
    private String appMasterJar;

    public YarnApplicationClient(Configuration configuration){
        this.configuration  = configuration;
        this.appMasterJar = ClassUtil.findContainingJar(ApplicationMaster.class);

        this.appMasterMainClass = ApplicationMaster.class.getName();

        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);
        yarnClient.start();
    }

    public void setAppMasterJar(String appMasterJar){
        this.appMasterJar = appMasterJar;
    }

    public ApplicationId submit() throws IOException, YarnException {

        FileSystem fileSystem = FileSystem.get(configuration);
        // 请求ResourceManager
        YarnClientApplication application =  yarnClient.createApplication();

        GetNewApplicationResponse getNewApplicationResponse = application.getNewApplicationResponse();

        Resource resource = getNewApplicationResponse.getMaximumResourceCapability(); // 集群最大资源数

        ApplicationSubmissionContext applicationSubmissionContext = application.getApplicationSubmissionContext();

        // define recource
        Resource amResource = Records.newRecord(Resource.class);
        amResource.setMemory(Math.min(resource.getMemory(), 1024));
        amResource.setVirtualCores(Math.min(resource.getVirtualCores(), 4));

        applicationSubmissionContext.setResource(amResource);

        // am
        ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("\"" + ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java\"")
                .append(" ")
                .append(appMasterMainClass)
                .append(" ")
                .append("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                        Path.SEPARATOR + ApplicationConstants.STDOUT)
                .append(" ")
                .append("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                Path.SEPARATOR + ApplicationConstants.STDERR);
        containerLaunchContext.setCommands(Collections.singletonList(stringBuilder.toString()));

        // add jar
        Map<String, LocalResource> localResourceMap = new HashMap<String, LocalResource>();

        File masterJarFile = new File(appMasterJar);

        localResourceMap.put(masterJarFile.getName(),toLocalResource(fileSystem, getNewApplicationResponse.getApplicationId().toString(),
                masterJarFile));

        // set environment variable
        Map<String, String> envMap = new HashMap<String, String>();

        envMap.put("CLASSPATH", hadoopClassPath());
        envMap.put("LANG", "en_US.UTF-8");

        containerLaunchContext.setEnvironment(envMap);

        applicationSubmissionContext.setAMContainerSpec(containerLaunchContext);

        return yarnClient.submitApplication(applicationSubmissionContext);
    }

    public  ApplicationReport getApplicationReport(ApplicationId applicationId) throws IOException, YarnException {

        return yarnClient.getApplicationReport(applicationId);
    }

    private String hadoopClassPath() {

        StringBuilder classPathEnv = new StringBuilder().append(File.pathSeparatorChar).append("./*");
        for (String c : configuration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }
        if (configuration.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(System.getProperty("java.class.path"));
        }
        return classPathEnv.toString();
    }
    private LocalResource toLocalResource(FileSystem fs, String appId, File file) throws IOException {
        Path hdfsFile = copyToHdfs(fs, appId, file.getPath());
        FileStatus stat = fs.getFileStatus(hdfsFile);
        return LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(hdfsFile.toUri()),
                LocalResourceType.FILE, LocalResourceVisibility.PRIVATE, stat.getLen(), stat.getModificationTime());
    }

    protected Path copyToHdfs(FileSystem fs, String appId, String srcFilePath) throws IOException {
        Path src = new Path(srcFilePath);
        String suffix = ".staging" + File.separator + appId + File.separator + src.getName();
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        if (!fs.exists(dst.getParent())) {
            FileSystem.mkdirs(fs, dst.getParent(), FsPermission.createImmutable((short) Integer.parseInt("755", 8)));
        }
        fs.copyFromLocalFile(src, dst);
        return dst;
    }
}
