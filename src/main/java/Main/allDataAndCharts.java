/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Main;


import java.awt.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import static java.sql.DriverManager.println;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;

import static java.sql.DriverManager.println;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.style.Styler.ChartTheme;

public class allDataAndCharts {
    
    public static void main(String[] args) {
        
        final SparkSession sparkSession = SparkSession.builder ().appName ("Spark CSV Analysis Demo").master ("local[2]")
                    .getOrCreate ();
        
        final DataFrameReader dataFrameReader = sparkSession.read ();
        
        dataFrameReader.option ("header", "true");
        
        final Dataset<Row> csvDataFrame = dataFrameReader.csv ("src/main/resources/Wuzzuf_Jobs.csv");
        
        csvDataFrame.select("Title", "Company", "Location", "Type", "Level", "YearsExp", "Country", "Skills").show(10);
        
        System.out.println("============================================= Schema =============================================");
        
        csvDataFrame.printSchema();
//        csvDataFrame.
        
        System.out.println("============================================= Describe =============================================");
        
        csvDataFrame.describe().show();
        
        System.out.println("========================================== dropDuplicates ============================================");
        
        csvDataFrame.dropDuplicates().show(10);
        
        System.out.println("========================================== na drop all ============================================");
        
        csvDataFrame.na().drop("all").show(10);
        
        System.out.println("========================================== ");
        
        csvDataFrame.show(10);
        
        System.out.println("==========================================");
        
        csvDataFrame.createOrReplaceTempView("num4");
        
        Dataset<Row> sqll = sparkSession.sql("select Company , count(Title) as CT  from num4 group by Company order by CT DESC limit 11 ");
        
        sqll.show(10);
        
        System.out.println("==========================================");
        
        
        sqll.createOrReplaceTempView("piee");
        
        List<String> companies = sqll.select("Company").as(Encoders.STRING()).collectAsList();
        
        List<Float> num_jobs = sqll.select("CT").as(Encoders.FLOAT()).collectAsList();
        
        PieChart chart = new PieChartBuilder ().width (800).height (600).title ("jobs").build ();
        
        Color[] sliceColors = new Color[]{new Color (180, 68, 50), new Color (130, 105, 120), new Color (80, 143, 160)};
        chart.getStyler ().setSeriesColors (sliceColors);
        chart.addSeries (companies.get(1), num_jobs.get (1));
        chart.addSeries (companies.get(2), num_jobs.get (2));
        chart.addSeries (companies.get(3), num_jobs.get (3));
        chart.addSeries (companies.get(4), num_jobs.get (4));
        chart.addSeries (companies.get(5), num_jobs.get (5));
        chart.addSeries (companies.get(6), num_jobs.get (6));
        chart.addSeries (companies.get(7), num_jobs.get (7));
      
        
        new SwingWrapper (chart).displayChart ();
        
        System.out.println("=========================================");
        
        csvDataFrame.createOrReplaceTempView("popular");
        
        Dataset<Row> mo_pop = sparkSession.sql("select * from (select Title , count(*) as x from popular group by Title order by x desc ) limit 10");
        
        
        
        mo_pop.show(10);
        
        List<String> job_titles = mo_pop.select("Title").as(Encoders.STRING()).collectAsList();
        
        List<Float> job_nums = mo_pop.select("x").as(Encoders.FLOAT()).collectAsList();
        
        CategoryChart chart2 = new CategoryChartBuilder ().width (1024).height (768).title ("most popular jobs").build ();
        
        chart2.addSeries("number of jibs", job_titles , job_nums);
        
        new SwingWrapper (chart2).displayChart ();
        
        System.out.println("=====================================");
        
        csvDataFrame.createOrReplaceTempView("pop_area");
        
        Dataset<Row> mo_pop_area = sparkSession.sql("select * from (select Location , count(*) as y from pop_area group by Location order by y desc) limit 10");
        
        List<String> loc = mo_pop_area.select("Location").as(Encoders.STRING()).collectAsList();
        
        List<Float> loc_pop = mo_pop_area.select("y").as(Encoders.FLOAT()).collectAsList();
        
        CategoryChart chart3 = new CategoryChartBuilder ().width (1024).height (768).title ("most popular area").build ();
        
        chart3.addSeries("location popularity", loc, loc_pop);
        
        new SwingWrapper (chart3).displayChart ();
        
 
    }
        
        
    
}
