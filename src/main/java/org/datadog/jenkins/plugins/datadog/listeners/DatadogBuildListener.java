/*
The MIT License

Copyright (c) 2015-Present Datadog, Inc <opensource@datadoghq.com>
All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
 */

package org.datadog.jenkins.plugins.datadog.listeners;

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;

import hudson.EnvVars;
import hudson.Extension;
import hudson.model.*;
import hudson.model.listeners.RunListener;
import org.datadog.jenkins.plugins.datadog.DatadogClient;
import org.datadog.jenkins.plugins.datadog.DatadogEvent;
import org.datadog.jenkins.plugins.datadog.DatadogUtilities;
import org.datadog.jenkins.plugins.datadog.clients.ClientFactory;
import org.datadog.jenkins.plugins.datadog.events.BuildAbortedEventImpl;
import org.datadog.jenkins.plugins.datadog.events.BuildFinishedEventImpl;
import org.datadog.jenkins.plugins.datadog.events.BuildStartedEventImpl;
import org.datadog.jenkins.plugins.datadog.model.BuildData;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * This class registers an {@link RunListener} to trigger events and calculate metrics:
 * - When a build starts, the {@link #onStarted(Run, TaskListener)} method will be invoked.
 * - When a build finishes, the {@link #onCompleted(Run, TaskListener)} method will be invoked.
 */
@Extension
public class DatadogBuildListener extends RunListener<Run>  {

    private static final Logger logger = Logger.getLogger(DatadogBuildListener.class.getName());

    /**
     * Called when a build is first started.
     *
     * @param run      - A Run object representing a particular execution of Job.
     * @param listener - A TaskListener object which receives events that happen during some
     *                 operation.
     */
    @Override
    public void onStarted(Run run, TaskListener listener) {
        try {
            // Process only if job is NOT in blacklist and is in whitelist
            if (!DatadogUtilities.isJobTracked(run.getParent().getFullName())) {
                return;
            }
            logger.fine("End DatadogBuildListener#onStarted");

            // Get Datadog Client Instance
            DatadogClient client = getDatadogClient();

            // Collect Build Data
            BuildData buildData;
            try {
                buildData = new BuildData(run, listener);
            } catch (IOException | InterruptedException e) {
                DatadogUtilities.severe(logger, e, null);
                return;
            }

            // Send an event
            DatadogEvent event = new BuildStartedEventImpl(buildData);
            client.event(event);

            // Send an metric
            // item.getInQueueSince() may raise a NPE if a worker node is spinning up to run the job.
            // This could be expected behavior with ec2 spot instances/ecs containers, meaning no waiting
            // queue times if the plugin is spinning up an instance/container for one/first job.
            Queue queue = getQueue();
            Queue.Item item = queue.getItem(run.getQueueId());
            Map<String, Set<String>> tags = buildData.getTags();
            String hostname = buildData.getHostname("null");
            try {
                long waiting = (DatadogUtilities.currentTimeMillis() - item.getInQueueSince()) / 1000;
                client.gauge("jenkins.job.waiting", waiting, hostname, tags);
            } catch (NullPointerException e) {
                logger.warning("Unable to compute 'waiting' metric. " +
                        "item.getInQueueSince() unavailable, possibly due to worker instance provisioning");
            }

            // Submit counter
            client.incrementCounter("jenkins.job.started", hostname, tags);

            logger.fine("End DatadogBuildListener#onStarted");
        } catch (Exception e) {
            DatadogUtilities.severe(logger, e, "An unexpected error occurred: ");
        }
    }

    /**
     * Called when a build is completed.
     *
     * @param run      - A Run object representing a particular execution of Job.
     * @param listener - A TaskListener object which receives events that happen during some
     *                 operation.
     */

    @Override
    public void onCompleted(Run run, @Nonnull TaskListener listener) {
        try {
            // Process only if job in NOT in blacklist and is in whitelist
            if (!DatadogUtilities.isJobTracked(run.getParent().getFullName())) {
                return;
            }
            logger.fine("Start DatadogBuildListener#onCompleted");

            // Get Datadog Client Instance
            DatadogClient client = getDatadogClient();

            // Collect Build Data
            BuildData buildData;
            try {
                buildData = new BuildData(run, listener);
            } catch (IOException | InterruptedException e) {
                DatadogUtilities.severe(logger, e, null);
                return;
            }

            // Send an event
            DatadogEvent event = new BuildFinishedEventImpl(buildData);
            client.event(event);

            // Send a metric
            Map<String, Set<String>> tags = buildData.getTags();
            String hostname = buildData.getHostname("null");
            client.gauge("jenkins.job.duration", buildData.getDuration(0L) / 1000, hostname, tags);

            // Submit counter
            client.incrementCounter("jenkins.job.completed", hostname, tags);

            // Send a service check
            String buildResult = buildData.getResult(Result.NOT_BUILT.toString());
            DatadogClient.Status status = DatadogClient.Status.UNKNOWN;
            if (Result.SUCCESS.toString().equals(buildResult)) {
                status = DatadogClient.Status.OK;
            } else if (Result.UNSTABLE.toString().equals(buildResult) ||
                    Result.ABORTED.toString().equals(buildResult) ||
                    Result.NOT_BUILT.toString().equals(buildResult)) {
                status = DatadogClient.Status.WARNING;
            } else if (Result.FAILURE.toString().equals(buildResult)) {
                status = DatadogClient.Status.CRITICAL;
            }
            client.serviceCheck("jenkins.job.status", status, hostname, tags);

            if (run.getResult() == Result.SUCCESS) {
                long mttr = getMeanTimeToRecovery(run);
                long cycleTime = getCycleTime(run);
                long leadTime = run.getDuration() + mttr;

                client.gauge("jenkins.job.leadtime", leadTime / 1000, hostname, tags);
                if (cycleTime > 0) {
                    client.gauge("jenkins.job.cycletime", cycleTime / 1000, hostname, tags);
                }
                if (mttr > 0) {
                    client.gauge("jenkins.job.mttr", mttr / 1000, hostname, tags);
                }
            } else {
                long feedbackTime = run.getDuration();
                long mtbf = getMeanTimeBetweenFailure(run);

                client.gauge("jenkins.job.feedbacktime", feedbackTime / 1000, hostname, tags);
                if (mtbf > 0) {
                    client.gauge("jenkins.job.mtbf", mtbf / 1000, hostname, tags);
                }
            }

            if ( DatadogUtilities.getDatadogGlobalDescriptor().getSendSnowflake() ) {
                try {
                    AmazonKinesisFirehose firehose = AmazonKinesisFirehoseClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
                    String snowflakedata = gatherSnowflakeData(run, buildData, listener);
                    byte[] bytes = snowflakedata.getBytes(StandardCharsets.UTF_8);
                    Record record = new Record().withData(ByteBuffer.wrap(bytes));
                    PutRecordRequest request = new PutRecordRequest().withDeliveryStreamName("jenkins-build-results").withRecord(record);
                    firehose.putRecord(request);
                    logger.fine("Sent snowflake data");
                } catch (Exception e) {
                    logger.severe(e.getMessage());
                }
            }
            logger.fine("End DatadogBuildListener#onCompleted");
        } catch (Exception e) {
            DatadogUtilities.severe(logger, e, "An unexpected error occurred: ");
        }
    }

    @Override
    public void onDeleted(Run run) {
        try {
            // Process only if job is NOT in blacklist and is in whitelist
            if (!DatadogUtilities.isJobTracked(run.getParent().getFullName())) {
                return;
            }
            logger.fine("Start DatadogBuildListener#onDeleted");

            // Get Datadog Client Instance
            DatadogClient client = getDatadogClient();

            // Collect Build Data
            BuildData buildData;
            try {
                buildData = new BuildData(run, null);
            } catch (IOException | InterruptedException e) {
                DatadogUtilities.severe(logger, e, null);
                return;
            }

            // Get the list of global tags to apply
            String hostname = buildData.getHostname("null");

            // Send an event
            DatadogEvent event = new BuildAbortedEventImpl(buildData);
            client.event(event);

            // Submit counter
            Map<String, Set<String>> tags = buildData.getTags();
            client.incrementCounter("jenkins.job.aborted", hostname, tags);

            logger.fine("End DatadogBuildListener#onDeleted");
        } catch (Exception e) {
            DatadogUtilities.severe(logger, e, "An unexpected error occurred: ");
        }
    }

    private long getMeanTimeBetweenFailure(Run<?, ?> run) {
        Run<?, ?> lastGreenRun = run.getPreviousNotFailedBuild();
        if (lastGreenRun != null) {
            return DatadogUtilities.getRunStartTimeInMillis(run) -
                    DatadogUtilities.getRunStartTimeInMillis(lastGreenRun);
        }
        return 0;
    }

    private long getCycleTime(Run<?, ?> run) {
        Run<?, ?> previousSuccessfulBuild = run.getPreviousSuccessfulBuild();
        if (previousSuccessfulBuild != null) {
            return (DatadogUtilities.getRunStartTimeInMillis(run) + run.getDuration()) -
                    (DatadogUtilities.getRunStartTimeInMillis(previousSuccessfulBuild) +
                            previousSuccessfulBuild.getDuration());
        }
        return 0;
    }

    private long getMeanTimeToRecovery(Run<?, ?> run) {
        if (isFailedBuild(run.getPreviousBuiltBuild())) {
            Run<?, ?> firstFailedRun = run.getPreviousBuiltBuild();

            while (firstFailedRun != null && isFailedBuild(firstFailedRun.getPreviousBuiltBuild())) {
                firstFailedRun = firstFailedRun.getPreviousBuiltBuild();
            }
            if (firstFailedRun != null) {
                return DatadogUtilities.getRunStartTimeInMillis(run) -
                        DatadogUtilities.getRunStartTimeInMillis(firstFailedRun);
            }
        }
        return 0;
    }


    private boolean isFailedBuild(Run<?, ?> run) {
        return run != null && run.getResult() != Result.SUCCESS;
    }

    public Queue getQueue(){
        return Queue.getInstance();
    }

    public DatadogClient getDatadogClient(){
        return ClientFactory.getClient();
    }

    /**
     * Returns a string containing the data we need to send to Snowflake. 
     * @param run - A Run object representing a particular execution of Job.
     * @param buildData - A JSONObject containing a builds metadata.
     * @param listener - A TaskListener object which receives events that happen during some
     *                   operation.
     * @return a String containing information formatted for Snowflake
     */
    private String gatherSnowflakeData(final Run run, BuildData buildData, @Nonnull final TaskListener listener) {
        ArrayList<String> snowflakelist = new ArrayList<String>();
        snowflakelist.add(Long.toString(buildData.getStartTime(0L) / 1000));
        snowflakelist.add(Long.toString(buildData.getEndTime(0L) / 1000));
        snowflakelist.add(buildData.getResult("").toString());
        snowflakelist.add(Long.toString(buildData.getDuration(0L) / 1000));
        Queue queue = Queue.getInstance();
        Queue.Item item = queue.getItem(run.getQueueId());
        if ( item != null ) {
            snowflakelist.add(Long.toString((run.getStartTimeInMillis() - item.getInQueueSince()) / 1000L));
        } else {
            logger.warning("Unable to compute 'waiting' metric for Snowflake. item.getInQueueSince() unavailable, possibly due to worker instance provisioning");
            snowflakelist.add("");
        }
        try {
            EnvVars envVars = run.getEnvironment(listener);
            if ( envVars.get("LUCID_GIT_BRANCH") != null ) {
                snowflakelist.add(envVars.get("LUCID_GIT_BRANCH").toString());
            } else if ( envVars.get("GIT_BRANCH") != null ) {
                snowflakelist.add(envVars.get("GIT_BRANCH").toString());
            } else if ( envVars.get("CVS_BRANCH") != null ) {
                snowflakelist.add(envVars.get("CVS_BRANCH").toString());
            } else {
                snowflakelist.add("");
            }
        } catch (IOException e) {
            logger.severe(e.getMessage());
        } catch (InterruptedException e) {
            logger.severe(e.getMessage());
        }
        String jobName = run.getParent().getFullName().replaceAll("»", "/").replaceAll(" ", "");;
        String pattern = "main/.*?/(.*)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(jobName);
        if ( m.find() ) {
            snowflakelist.add(m.group(1));
        } else {
            snowflakelist.add(jobName);
        }
        snowflakelist.add(buildData.getBuildNumber("").toString());

        String snowflakedata = StringUtils.join(snowflakelist, (char) 1) + "\n";
        return snowflakedata;
    }
}
