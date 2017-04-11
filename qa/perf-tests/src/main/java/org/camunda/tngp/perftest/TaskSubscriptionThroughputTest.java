package org.camunda.tngp.perftest;

import static org.camunda.tngp.client.ClientProperties.CLIENT_MAXREQUESTS;
import static org.camunda.tngp.client.ClientProperties.CLIENT_MAXCONNECTIONS;
import static org.camunda.tngp.client.ClientProperties.CLIENT_TASK_EXECUTION_THREADS;
import static org.camunda.tngp.perftest.helper.TestHelper.printProperties;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.camunda.tngp.client.ClientProperties;
import org.camunda.tngp.client.TngpClient;
import org.camunda.tngp.client.task.TaskSubscription;
import org.camunda.tngp.perftest.helper.TestHelper;
import org.camunda.tngp.perftest.reporter.FileReportWriter;
import org.camunda.tngp.perftest.reporter.RateReporter;
import org.camunda.tngp.transport.requestresponse.client.TransportConnection;

public class TaskSubscriptionThroughputTest
{

    public static final String TEST_NUM_TASKS = "test.tasks";
    public static final String TEST_SETUP_TIMEMS = "test.setup.timems";

    public static final String TASK_TYPE = "foo";

    public static void main(String[] args)
    {
        new TaskSubscriptionThroughputTest().run();
    }

    public void run()
    {
        final Properties properties = System.getProperties();
        properties.putIfAbsent(CLIENT_MAXREQUESTS, "2048");
        properties.putIfAbsent(CLIENT_TASK_EXECUTION_THREADS, "8");
        properties.putIfAbsent(CLIENT_MAXCONNECTIONS, "16");
        ClientProperties.setDefaults(properties);
        setDefaultProperties(properties);

        printProperties(properties);

        TngpClient client = null;

        try
        {
            client = TngpClient.create(properties);
            client.connect();

            executeSetup(properties, client);
            executeTest(properties, client);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            client.close();
        }
    }

    protected void setDefaultProperties(Properties properties)
    {
        properties.putIfAbsent(CommonProperties.TEST_OUTPUT_FILE_NAME, "data/output.txt");
        properties.putIfAbsent(CommonProperties.TEST_TIMEMS, "30000");
        properties.putIfAbsent(TEST_NUM_TASKS, "50000");
        properties.putIfAbsent(TEST_SETUP_TIMEMS, "15000");
    }

    private void executeTest(Properties properties, TngpClient client) throws InterruptedException
    {

        final int testTimeMs = Integer.parseInt(properties.getProperty(CommonProperties.TEST_TIMEMS));
        final String outFile = properties.getProperty(CommonProperties.TEST_OUTPUT_FILE_NAME);

        final FileReportWriter fileReportWriter = new FileReportWriter();
        final RateReporter reporter = new RateReporter(1, TimeUnit.SECONDS, fileReportWriter);


        new Thread()
        {
            @Override
            public void run()
            {
                reporter.doReport();
            }

        }.start();

        final long testTimeNanos = TimeUnit.MILLISECONDS.toNanos(testTimeMs);

        final TaskSubscription subscription = client.taskTopic(0).newTaskSubscription()
            .lockTime(10000)
            .lockOwner(0)
            .taskFetchSize(10000)
            .taskType(TASK_TYPE)
            .handler((t) ->
            {
                reporter.increment();
            })
            .open();

        LockSupport.parkNanos(testTimeNanos);

        subscription.close();

        reporter.exit();

        fileReportWriter.writeToFile(outFile);
    }

    private void executeSetup(Properties properties, TngpClient client)
    {
        final int numTasks = Integer.parseInt(properties.getProperty(TEST_NUM_TASKS));
        final int setUpTimeMs = Integer.parseInt(properties.getProperty(TEST_SETUP_TIMEMS));

        try (TransportConnection connection = client.getConnectionPool().openConnection())
        {
            final Supplier<Future> request = () -> client.taskTopic(0).create()
                    .taskType(TASK_TYPE)
                    .executeAsync(connection);

            TestHelper.executeAtFixedRate(
                request,
                (l) ->
                { },
                numTasks / (int) TimeUnit.MILLISECONDS.toSeconds(setUpTimeMs),
                setUpTimeMs);
        }
    }
}
