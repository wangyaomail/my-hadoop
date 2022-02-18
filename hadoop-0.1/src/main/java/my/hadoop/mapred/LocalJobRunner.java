/**
 * Copyright 2005 The Apache Software Foundation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package my.hadoop.mapred;

import my.hadoop.conf.Configuration;
import my.hadoop.fs.FileSystem;
import my.hadoop.util.LogFormatter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements MapReduce locally, in-process, for debugging.
 */
class LocalJobRunner implements JobSubmissionProtocol {
    public static final Logger LOG = LogFormatter.getLogger(
            "org.apache.hadoop.mapred.LocalJobRunner");

    private FileSystem fs;
    private HashMap jobs = new HashMap();
    private Configuration conf;
    private int map_tasks = 0;
    private int reduce_tasks = 0;

    private class Job extends Thread implements TaskUmbilicalProtocol {
        private String file;
        private String id;
        private JobConf job;

        private JobStatus status = new JobStatus();
        private ArrayList mapIds = new ArrayList();
        private MapOutputFile mapoutputFile;
        private JobProfile profile;
        private File localFile;
        private FileSystem localFs;

        public Job(String file, Configuration conf) throws IOException {
            this.file = file;
            this.id = "job_" + newId();
            this.mapoutputFile = new MapOutputFile();
            this.mapoutputFile.setConf(conf);

            this.localFile = new JobConf(conf).getLocalFile("localRunner", id + ".xml");
            this.localFs = FileSystem.getNamed("local", conf);

            fs.copyToLocalFile(new File(file), localFile);
            this.job = new JobConf(localFile);
            profile = new JobProfile(job.getUser(),
                                     id,
                                     file,
                                     "http://localhost:8080/",
                                     job.getJobName());
            this.status.jobid = id;
            this.status.runState = JobStatus.RUNNING;

            jobs.put(id, this);

            this.start();
        }

        JobProfile getProfile() {
            return profile;
        }

        private void setWorkingDirectory(JobConf conf, FileSystem fs) {
            String dir = conf.getWorkingDirectory();
            if (dir != null) {
                fs.setWorkingDirectory(new File(dir));
            }
        }

        public void run() {
            try {
                // split input into minimum number of splits
                FileSplit[] splits;
                setWorkingDirectory(job, fs);
                splits = job.getInputFormat().getSplits(fs, job, 1);

                // run a map task for each split
                job.setNumReduceTasks(1);                 // force a single reduce task
                for (int i = 0; i < splits.length; i++) {
                    mapIds.add("map_" + newId());
                    MapTask map = new MapTask(file, (String) mapIds.get(i), splits[i]);
                    map.setConf(job);
                    map_tasks += 1;
                    map.run(job, this);
                    map_tasks -= 1;
                }

                // move map output to reduce input
                String reduceId = "reduce_" + newId();
                for (int i = 0; i < mapIds.size(); i++) {
                    String mapId = (String) mapIds.get(i);
                    File mapOut = this.mapoutputFile.getOutputFile(mapId, 0);
                    File reduceIn = this.mapoutputFile.getInputFile(mapId, reduceId);
                    reduceIn.getParentFile().mkdirs();
                    if (!localFs.rename(mapOut, reduceIn))
                        throw new IOException("Couldn't rename " + mapOut);
                    this.mapoutputFile.removeAll(mapId);
                }

                // run a single reduce task
                String mapDependencies[][] = new String[mapIds.size()][1];
                for (int i = 0; i < mapIds.size(); i++) {
                    mapDependencies[i][0] = (String) mapIds.get(i);
                }
                setWorkingDirectory(job, fs);
                ReduceTask reduce = new ReduceTask(file, reduceId, mapDependencies, 0);
                reduce.setConf(job);
                reduce_tasks += 1;
                reduce.run(job, this);
                reduce_tasks -= 1;
                this.mapoutputFile.removeAll(reduceId);

                this.status.runState = JobStatus.SUCCEEDED;

            } catch (Throwable t) {
                this.status.runState = JobStatus.FAILED;
                LOG.log(Level.WARNING, id, t);

            } finally {
                try {
                    fs.delete(new File(file).getParentFile()); // delete submit dir
                    localFs.delete(localFile);              // delete local copy
                } catch (IOException e) {
                    LOG.warning("Error cleaning up " + id + ": " + e);
                }
            }
        }

        private String newId() {
            return Integer.toString(Math.abs(new Random().nextInt()), 36);
        }

        // TaskUmbilicalProtocol methods

        public Task getTask(String taskid) {
            return null;
        }

        public void progress(String taskId, float progress, String state) {
            LOG.info(state);
            float taskIndex = mapIds.indexOf(taskId);
            if (taskIndex >= 0) {                       // mapping
                float numTasks = mapIds.size();
                status.mapProgress = (taskIndex / numTasks) + (progress / numTasks);
            } else {
                status.reduceProgress = progress;
            }
        }

        public void reportDiagnosticInfo(String taskid, String trace) {
            // Ignore for now
        }

        public void ping(String taskid) throws IOException {
        }

        public void done(String taskId) throws IOException {
            int taskIndex = mapIds.indexOf(taskId);
            if (taskIndex >= 0) {                       // mapping
                status.mapProgress = 1.0f;
            } else {
                status.reduceProgress = 1.0f;
            }
        }

        public synchronized void fsError(String message) throws IOException {
            LOG.severe("FSError: " + message);
        }

    }

    public LocalJobRunner(Configuration conf) throws IOException {
        this.fs = FileSystem.get(conf);
        this.conf = conf;
    }

    // JobSubmissionProtocol methods

    public JobStatus submitJob(String jobFile) throws IOException {
        return new Job(jobFile, this.conf).status;
    }

    public void killJob(String id) {
        ((Thread) jobs.get(id)).stop();
    }

    public JobProfile getJobProfile(String id) {
        Job job = (Job) jobs.get(id);
        return job.getProfile();
    }

    public TaskReport[] getMapTaskReports(String id) {
        return new TaskReport[0];
    }

    public TaskReport[] getReduceTaskReports(String id) {
        return new TaskReport[0];
    }

    public JobStatus getJobStatus(String id) {
        Job job = (Job) jobs.get(id);
        return job.status;
    }

    public String getFilesystemName() throws IOException {
        return fs.getName();
    }

    public ClusterStatus getClusterStatus() {
        return new ClusterStatus(1, map_tasks, reduce_tasks, 1);
    }
}
