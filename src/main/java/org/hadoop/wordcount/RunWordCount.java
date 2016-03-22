package org.hadoop.wordcount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by danielsarbe on 1/18/14.
 */
public class RunWordCount {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunWordCount.class);
    private static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";

    public static void main(String[] args) throws Exception {

        //Set the config file to be loaded
        String conf = "conf/hadoop-localhost.xml";
        String inputPath = "";

        if (conf.contains("-local.xml")) {
            inputPath = "src/main/resources/content/*.*";
        }
        if (conf.contains("-localhost.xml")) {
            inputPath = "content/*.*";
        }
        if (conf.contains("-remote.xml")) {
            System.setProperty(HADOOP_USER_NAME, "cloudera");
            inputPath= "content/2014/*.*";
        }

        String outputFolder = "target/wordcount/output_" + System.currentTimeMillis();
        LOGGER.info("Configuration used: <" + conf + ">. Output folder: <" + outputFolder + ">.");


        WordCount.main(
                new String[]{
                        "-conf", conf,
                        inputPath,
                        outputFolder
                });
    }
}
