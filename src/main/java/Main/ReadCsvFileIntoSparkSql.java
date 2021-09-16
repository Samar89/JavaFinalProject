/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Main;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.*;
/**
 *
 * @author samar
 */
public class ReadCsvFileIntoSparkSql {
    public List<Jobs> getData() {
        
        List<Jobs> allJobs = new ArrayList<Jobs>();
        
        final SparkSession sparkSession = SparkSession.builder ().appName ("Spark CSV Analysis Demo").master ("local[2]").getOrCreate ();

        final DataFrameReader dataFrameReader = sparkSession.read ();

        dataFrameReader.option ("header", "true");
        
        final Dataset<Row> csvDataFrame = dataFrameReader.format("csv").load("src/main/resources/Wuzzuf_Jobs.csv");
        
        csvDataFrame.createOrReplaceTempView("AllJobsData");
        
        Dataset<Row> getjobs = sparkSession.sql("select Title,Company,Location,Type,Level,YearsExp,Country,Skills from AllJobsData limit 10 "); 
        
        List<Row> jobsList = getjobs.collectAsList();

	for (Row row : jobsList) {
            allJobs.add(new Jobs(row.getString(0)));
        }

        Jobs createjob = new Jobs("First Job");

        allJobs.add(createjob);

        return allJobs;
    }
}
