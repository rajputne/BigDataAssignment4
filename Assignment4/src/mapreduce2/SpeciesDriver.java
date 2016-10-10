// 
// Author - Jack Hebert (jhebert@cs.washington.edu) 
// Copyright 2007 
// Distributed under GPLv3 
// 
// Modified - Dino Konstantopoulos
// Distributed under the "If it works, remolded by Dino Konstantopoulos, 
// otherwise no idea who did! And by the way, you're free to do whatever 
// you want to with it" dinolicense
// 
package mapreduce2;

import mapreducejava.*;
import java.io.File;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

// import org.apache.nutch.parse.Parse; 
// import org.apache.nutch.parse.ParseException; 
// import org.apache.nutch.parse.ParseUtil; 
// import org.apache.nutch.protocol.Content; 
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class SpeciesDriver {

    public static void main(String[] args) throws Exception {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(SpeciesDriver.class);
        conf.setJobName("Page-rank Species Graph Builder");
        final File f = new File(SpeciesDriver.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        String inFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/InputFiles/species_medium.txt";
        String outFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/outputFiles/Result";
        FileInputFormat.setInputPaths(conf, new Path(inFiles));
        FileOutputFormat.setOutputPath(conf, new Path(outFiles));

        //conf.setOutputKeyClass(Text.class); 
        //conf.setOutputValueClass(Text.class); 
        conf.setMapperClass(SpeciesGraphBuilderMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        //conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class); 
        //conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class); 
        conf.setReducerClass(SpeciesGraphBuilderReducer.class);
        //conf.setCombinerClass(SpeciesGraphBuilderReducer.class); 

        //conf.setInputPath(new Path("graph1")); 
        //conf.setOutputPath(new Path("graph2")); 
        // take the input and output from the command line
        FileInputFormat.setInputPaths(conf, new Path(inFiles));
        FileOutputFormat.setOutputPath(conf, new Path(outFiles));

        client.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

        
        inFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/outputFiles/Result/part-00000";
        for (int i = 0; i < 15; i++) {
            client = new JobClient();
            conf = new JobConf(SpeciesDriver.class);
            conf.setJobName("Species Iter");
          
            int count=i+1;
            outFiles=f.getAbsolutePath().replace("/build/classes", "") + "/src/outputFiles/Result"+count;
            conf.setNumReduceTasks(5);

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(conf, new Path(inFiles));
            FileOutputFormat.setOutputPath(conf, new Path(outFiles));

            conf.setMapperClass(SpeciesIterMapper2.class);
            conf.setReducerClass(SpeciesIterReducer2.class);
            conf.setCombinerClass(SpeciesIterReducer2.class);

            client.setConf(conf);
            try {
                JobClient.runJob(conf);
            } catch (Exception e) {
                e.printStackTrace();
            }
            inFiles=outFiles;
            
        }

      
        //Viewer
        client = new JobClient();
        conf = new JobConf(SpeciesDriver.class);
        conf.setJobName("Species Viewer");

        conf.setOutputKeyClass(FloatWritable.class);
        conf.setOutputValueClass(Text.class);

        inFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/outputFiles/Result15/part-00000";
        outFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/outputFiles/ResultFinal";

        FileInputFormat.setInputPaths(conf, new Path(inFiles));
        FileOutputFormat.setOutputPath(conf, new Path(outFiles));

        conf.setMapperClass(SpeciesViewerMapper.class);
        conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);

        client.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
