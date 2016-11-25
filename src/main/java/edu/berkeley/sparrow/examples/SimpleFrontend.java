/*
 * Copyright 2013 The Regents of The University California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.sparrow.examples;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.api.SparrowFrontendClient;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * Simple frontend that runs jobs composed of sleep tasks.
 */
public class SimpleFrontend implements FrontendService.Iface {
  /** Amount of time to launch tasks for. */
  public static final String EXPERIMENT_S = "experiment_s";
  public static final int DEFAULT_EXPERIMENT_S = 300;

  public static final String JOB_ARRIVAL_PERIOD_MILLIS = "job_arrival_period_millis";
  public static final int DEFAULT_JOB_ARRIVAL_PERIOD_MILLIS = 100;

  /** Number of tasks per job. */
  public static final String TASKS_PER_JOB = "tasks_per_job";
  public static final int DEFAULT_TASKS_PER_JOB = 1;

  /** Duration of one task, in milliseconds */
  public static final String TASK_DURATION_MILLIS = "task_duration_millis";
  public static final int DEFAULT_TASK_DURATION_MILLIS = 100;

  /** Host and port where scheduler is running. */
  public static final String SCHEDULER_HOST = "scheduler_host";
  public static final String DEFAULT_SCHEDULER_HOST = "localhost";
  public static final String SCHEDULER_PORT = "scheduler_port";

  /**
   * Default application name.
   */
  public static final String APPLICATION_ID = "sleepApp";

  private static final Logger LOG = Logger.getLogger(SimpleFrontend.class);

  private static final TUserGroupInfo USER = new TUserGroupInfo();

  private SparrowFrontendClient client;

  /** A runnable which Spawns a new thread to launch a scheduling request. */
  private class JobLaunchRunnable implements Runnable {
    private int tasksPerJob;
    private int taskDurationMillis;

    public JobLaunchRunnable(int tasksPerJob, int taskDurationMillis) {
      this.tasksPerJob = tasksPerJob;
      this.taskDurationMillis = taskDurationMillis;
    }

    @Override
    public void run() {
      // Generate tasks in the format expected by Sparrow. First, pack task parameters.
      ByteBuffer message = ByteBuffer.allocate(4);
      message.putInt(taskDurationMillis);

      List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
      for (int taskId = 0; taskId < tasksPerJob; taskId++) {
        TTaskSpec spec = new TTaskSpec();
        spec.setTaskId(Integer.toString(taskId));
        spec.setMessage(message.array());
        tasks.add(spec);
      }
      long start = System.currentTimeMillis();
      try {
        client.submitJob(APPLICATION_ID, tasks, USER);
      } catch (TException e) {
        LOG.error("Scheduling request failed!", e);
      }
      long end = System.currentTimeMillis();
      LOG.debug("Scheduling request duration " + (end - start));
    }
  }

  public void run(String[] args) {
    try {
      OptionParser parser = new OptionParser();
      parser.accepts("c", "configuration file").withRequiredArg().ofType(String.class);
      parser.accepts("f", "configuration file").withRequiredArg().ofType(String.class);
      parser.accepts("l", "configuration file").withRequiredArg().ofType(Integer.class);
      parser.accepts("help", "print help statement");
      OptionSet options = parser.parse(args);
      String filename = null;
      int line = 0;

      if (options.has("help")) {
        parser.printHelpOn(System.out);
        System.exit(-1);
      }

      if (options.has("f")) {
	filename = (String) options.valueOf("f");
      }else{
        parser.printHelpOn(System.out);
        System.exit(-1);
      }

      if (options.has("l")) {
	Integer temp = (Integer) options.valueOf("l");
	line = temp.intValue();
      }else{
        parser.printHelpOn(System.out);
        System.exit(-1);
      }

      // Logger configuration: log to the console
      BasicConfigurator.configure();
      LOG.setLevel(Level.DEBUG);

      Configuration conf = new PropertiesConfiguration();

      if (options.has("c")) {
        String configFile = (String) options.valueOf("c");
        conf = new PropertiesConfiguration(configFile);
      }

      int arrivalPeriodMillis = conf.getInt(JOB_ARRIVAL_PERIOD_MILLIS,
          DEFAULT_JOB_ARRIVAL_PERIOD_MILLIS);
      int experimentDurationS = conf.getInt(EXPERIMENT_S, DEFAULT_EXPERIMENT_S);
      LOG.debug("Using arrival period of " + arrivalPeriodMillis +
          " milliseconds and running experiment for " + experimentDurationS + " seconds.");
//      int tasksPerJob = conf.getInt(TASKS_PER_JOB, DEFAULT_TASKS_PER_JOB);
 //     int taskDurationMillis = conf.getInt(TASK_DURATION_MILLIS, DEFAULT_TASK_DURATION_MILLIS);

      int schedulerPort = conf.getInt(SCHEDULER_PORT,
          SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT);
      String schedulerHost = conf.getString(SCHEDULER_HOST, DEFAULT_SCHEDULER_HOST);
      client = new SparrowFrontendClient();
      client.initialize(new InetSocketAddress(schedulerHost, schedulerPort), APPLICATION_ID, this);

  //    JobLaunchRunnable runnable = new JobLaunchRunnable(tasksPerJob, taskDurationMillis);
   //   ScheduledThreadPoolExecutor taskLauncher = new ScheduledThreadPoolExecutor(1);
//      taskLauncher.scheduleAtFixedRate(runnable, 0, arrivalPeriodMillis, TimeUnit.MILLISECONDS);
      int delayPerJob[] = new int[line];
      int tasksPerJob[] = new int[line];
      int taskDurationMillis[] = new int[line];
      JobLaunchRunnable runnable[] = new JobLaunchRunnable[line];
      File file = new File(filename);
      BufferedReader reader = null;
      try {
        //System.out.println("以行为单位读取文件内容，一次读一整行：");
        reader = new BufferedReader(new FileReader(file));
        String tempString = null;
        int tline = 0;
        // 一次读入一行，直到读入null为文件结束
        while ((tempString = reader.readLine()) != null) {
          // 显示行号
          //System.out.println("line " + line + ": " + tempString);
          if(tline < line){
             String[] strArray = tempString.split(" ");
             delayPerJob[tline] = Integer.valueOf(strArray[0]).intValue();
             tasksPerJob[tline] = Integer.valueOf(strArray[1]).intValue();
	     double tempduration = Double.valueOf(strArray[2]).doubleValue();
             taskDurationMillis[tline] = (int)(tempduration*100);
          }
          tline++;
        }
        reader.close();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (Exception e1) {
          }
        }
      }
      for(int i=0;i<line;i++){
          runnable[i] = new JobLaunchRunnable(tasksPerJob[i],taskDurationMillis[i]);
      }
   //   JobLaunchRunnable runnable = new JobLaunchRunnable(tasksPerJob, taskDurationMillis);

      ScheduledThreadPoolExecutor taskLauncher = new ScheduledThreadPoolExecutor(1);
      for(int i = 0;i<line;i++){
        taskLauncher.schedule(runnable[i], delayPerJob[i], TimeUnit.MILLISECONDS);
      }

      long startTime = System.currentTimeMillis();
      LOG.debug("sleeping");
      while (System.currentTimeMillis() < startTime + experimentDurationS * 1000) {
        Thread.sleep(100);
      }
      taskLauncher.shutdown();
    }
    catch (Exception e) {
      LOG.error("Fatal exception", e);
    }
  }

  @Override
  public void frontendMessage(TFullTaskId taskId, int status, ByteBuffer message)
      throws TException {
    // We don't use messages here, so just log it.
    LOG.debug("Got unexpected message: " + Serialization.getByteBufferContents(message));
  }

  public static void main(String[] args) {
    new SimpleFrontend().run(args);
  }
}
