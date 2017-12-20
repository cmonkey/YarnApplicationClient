package org.github.cmonkey.yarn.test;

import org.github.cmonkey.yarn.YarnApplicationClient;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestYarnApplicationClient {

    private YarnConfiguration yarnConfiguration;
    @Before
    public void before() throws Exception{

        yarnConfiguration = new YarnConfiguration();

        yarnConfiguration.set(YarnConfiguration.RM_ADDRESS, "localhost:8032");
        yarnConfiguration.set(YarnConfiguration.RM_HOSTNAME, "localhost");
        yarnConfiguration.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, "localhost:8030");
        yarnConfiguration.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "localhost:8031");
        yarnConfiguration.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:8088");
        yarnConfiguration.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);

        MiniYARNCluster miniYARNCluster = new MiniYARNCluster("test", 1, 1, 1);
        miniYARNCluster.init(yarnConfiguration);
        miniYARNCluster.start();

    }
    @Test
    public void testClient() throws IOException, YarnException, InterruptedException {
        YarnApplicationClient client = new YarnApplicationClient(yarnConfiguration);
        client.setAppMasterJar("target/yarn-1.0-SNAPSHOT.jar");
        ApplicationId applicationId = client.submit();
        boolean outAccepted = false;
        boolean outTrackingUrl = false;
        ApplicationReport report = client.getApplicationReport(applicationId);
        while (report.getYarnApplicationState() != YarnApplicationState.FINISHED) {
            report = client.getApplicationReport(applicationId);
            if (!outAccepted && report.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
                System.out.println("Application is accepted use Queue=" + report.getQueue()
                        + " applicationId=" + report.getApplicationId());
                outAccepted = true;
            }
            if (!outTrackingUrl && report.getYarnApplicationState() == YarnApplicationState.RUNNING) {
                String trackingUrl = report.getTrackingUrl();
                System.out.println("Master Tracking URL = " + trackingUrl);
                outTrackingUrl = true;
            }
            System.out.println(String.format("%f %s", report.getProgress(), report.getYarnApplicationState()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(report.getFinalApplicationStatus());
        while (true) {
            Thread.sleep(1000);
        }
    }
}
