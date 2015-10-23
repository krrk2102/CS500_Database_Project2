package edu.drexel.cs500.motif;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.api.java.JavaPairRDD;

/**
 * @author Haoyue Ping, Shangqi Wu
 * Frequent preference motif mining with Apache Spark SQL.
 */
public final class Motif {

    // global variables
    private static JavaSparkContext sparkContext;
    private static SQLContext sqlContext;

    // Set up Spark and SQL contexts.
    private static void init (String master, int numReducers) {

        // forbid logging / recording ?
        Logger.getRootLogger().setLevel(Level.OFF);

        // get sparkConf from input parameters
        SparkConf sparkConf = new SparkConf().setAppName("Motif")
                                             .setMaster(master)
                                             .set("spark.sql.shuffle.partitions", "" + numReducers);

        sparkContext = new JavaSparkContext(sparkConf);
        sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
    }

    // transform transaction file into DataFrame
    private static DataFrame initPref (String inFileName) {

        // read in the transactions file
        JavaRDD<String> prefRDD = sparkContext.textFile(inFileName);    // unstructured lines

        // establish the schema: PREF (tid: string, item1: int, item2: int)
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("tid", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("item1", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("item2", DataTypes.IntegerType, true));
        StructType prefSchema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = prefRDD.map(new Function<String, Row>() {
                                            static final long serialVersionUID = 42L; // ???
                                            public Row call(String record) throws Exception {
                                                String[] fields = record.split("\t"); // \t is tab
                                                return RowFactory.create(fields[0],
                                                Integer.parseInt(fields[1].trim()),
                                                Integer.parseInt(fields[2].trim()));}});

        // create DataFrame from prefRDD, with the specified schema
        return sqlContext.createDataFrame(rowRDD, prefSchema);
    }

    private static void saveOutput (DataFrame df, String outDir, String outFile) throws IOException {

        File outF = new File(outDir);
            outF.mkdirs();
            BufferedWriter outFP = new BufferedWriter(new FileWriter(outDir + "/" + outFile));

        List<Row> rows = df.toJavaRDD().collect();
        for (Row r : rows) {
            outFP.write(r.toString() + "\n");
        }

        outFP.close();

    }

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.err.println("Usage: Motif <inFile> <support> <outDir> <master> <numReducers>");
            System.exit(1);
        }

        // get input parameters for current task
        String inFileName = args[0].trim();
        double thresh =  Double.parseDouble(args[1].trim());    // supp
        String outDirName = args[2].trim();
        String master = args[3].trim();
        int numReducers = Integer.parseInt(args[4].trim());

        // get sparkContext & sqlContext
        Motif.init(master, numReducers);
        Logger.getRootLogger().setLevel(Level.OFF);
        DataFrame pref = Motif.initPref(inFileName);

        //////////////////////////////////////////////////////////////////////
        // tid list ["T1", "T2", "T3"]
        List<String> tidList = pref.groupBy("tid").count().select("tid").toJavaRDD().map(
                                            new Function<Row, String>() {
                                                @Override
                                                public String call(Row row) {
                                                    return row.getString(0);
                                                }}).collect();

        int tidCount = tidList.size();  // number of tids
        int supp = (int)Math.floor(thresh*tidCount);
        System.out.println(supp);

        // -- update every transitive edge and get a new DataFrame prefUpdate --
        DataFrame prefCopy = pref.toDF("tidc", "item1c", "item2c");
        DataFrame prefUpdate = pref;
        for (String tid: tidList) {
            DataFrame tmpPrefCopy = prefCopy.filter(prefCopy.col("tidc").equalTo(tid));
            DataFrame tmpPrefUpdate = pref.filter(pref.col("tid").equalTo(tid));
            DataFrame tmpNewPrefUpdate = null;
            Long tmpLength1,tmpLength2;
            do {
                tmpNewPrefUpdate = tmpPrefUpdate.join(tmpPrefCopy, tmpPrefUpdate.col("item2").equalTo(tmpPrefCopy.col("item1c")));
                tmpNewPrefUpdate = tmpNewPrefUpdate.select("tid","item1","item2c");
                tmpNewPrefUpdate = tmpNewPrefUpdate.toDF("tid","item1","item2");
                tmpLength1 = tmpPrefUpdate.count();
                tmpPrefUpdate = tmpPrefUpdate.unionAll(tmpNewPrefUpdate).distinct();
                tmpLength2 = tmpPrefUpdate.count();
            }while(tmpLength2 > tmpLength1);
            prefUpdate = prefUpdate.unionAll(tmpPrefUpdate);
        }
        prefUpdate = prefUpdate.distinct();

        //////////////////////////////////////////////////////////////////////
        // 4 different ways of combine 2 edges. by 4 different kind of join.
        // test all combined motifs.
        // test all popular edges.
        DataFrame countTable = prefUpdate.groupBy("item1","item2").count();
        countTable = countTable.filter(countTable.col("count").gt(supp)).select("item1","item2");
        countTable = countTable.toDF("item1freq", "item2freq");

        // Next Step: find popular edges in each tid
        prefUpdate.registerTempTable("prefUpdate");
        countTable.registerTempTable("countTable");

        String sqlString = "SELECT * FROM prefUpdate P right outer join countTable C where P.item1 = C.item1freq and P.item2 = C.item2freq";
        DataFrame tmpResult = sqlContext.sql(sqlString);    // SQL query

        // Next Step: find popular Motifs by sql join for each tid
        pref = tmpResult.select("tid", "item1", "item2");
        DataFrame prefcopy = pref.toDF("tidc", "item1c", "item2c");

        DataFrame lMotifs = pref.join(prefcopy, pref.col("item2").equalTo(prefcopy.col("item1c")))
                            .filter("tid = tidc and item1c is not null and item2c is not null");
        lMotifs = lMotifs.select("item1", "item2", "item2c").groupBy("item1", "item2", "item2c").count();
        lMotifs = lMotifs.filter(lMotifs.col("count").gt(supp));
        // vMotifs: left outer join on P1.tid=P2.tid and P1.item2=P2.item2;
        DataFrame vMotifs = pref.join(prefcopy, pref.col("item2").equalTo(prefcopy.col("item2c")))
                            .filter("tid = tidc and item1 < item1c and item1c is not null and item2c is not null");
        vMotifs = vMotifs.select("item1", "item2", "item1c").groupBy("item1", "item2", "item1c").count();
        vMotifs = vMotifs.filter(vMotifs.col("count").gt(supp));
        // aMotifs: left outer join on P1.tid=P2.tid and P1.item1=P2.item1;
        DataFrame aMotifs = pref.join(prefcopy, pref.col("item1").equalTo(prefcopy.col("item1c")))
                            .filter("tid = tidc and item2 < item2c and item1c is not null and item2c is not null");
        aMotifs = aMotifs.select("item2", "item1", "item2c").groupBy("item2", "item1", "item2c").count();
        aMotifs = aMotifs.filter(aMotifs.col("count").gt(supp));

        try {
            Motif.saveOutput(lMotifs, outDirName + "/" + thresh, "L");
        } catch (IOException ioe) {
            System.out.println("Cound not output L-Motifs " + ioe.toString());
        }

        try {
            Motif.saveOutput(vMotifs, outDirName + "/" + thresh, "V");
        } catch (IOException ioe) {
            System.out.println("Cound not output V-Motifs " + ioe.toString());
        }

        try {
            Motif.saveOutput(aMotifs, outDirName + "/" + thresh, "A");
        } catch (IOException ioe) {
            System.out.println("Cound not output A-Motifs " + ioe.toString());
        }

        System.out.println("Done");
        sparkContext.stop();

    }
}
