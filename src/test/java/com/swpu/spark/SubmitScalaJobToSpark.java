package com.swpu.spark;

import org.apache.spark.deploy.SparkSubmit;

import java.text.SimpleDateFormat;
import java.util.Date;

public class SubmitScalaJobToSpark {

    public static void main(String[] args) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
        String fileName = dateFormat.format(new Date());

        String tmp = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        tmp = tmp.substring(0, tmp.length() - 8);

        String[] arg0 = new String[]{
                "--master", "spark://192.168.1.91",
                "--deploy-mode", "client",
                "--name", "test java submit job to spark",
                "--class", "Scala_Test",
                "--executor-memory", "1G",
                //"spark_filter.jar",
                tmp + "lib/spark_filter.jar",
                "hdfs://192.168.1.91:9000/samples/logs/log",
                "hdfs://192.168.1.91:9000/samples/logs/badLines_spark_" + fileName
        };

        SparkSubmit.main(arg0);
    }
}
