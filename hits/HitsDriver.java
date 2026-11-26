package hits;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HitsDriver {

    // (tranche de 15 min, 1)
    public static class HitsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable UN = new IntWritable(1);
        private static final int DEBUT = 10 * 60 + 30;   // 10h30
        private static final int FIN   = 23 * 60 + 30;   // dernière tranche affichée par le corrigé : 23:30

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 79.133.215.123 - - [14/Jun/2014:10:30:13 -0400] "GET /home ..."
            String[] champs = value.toString().split(" ");
            if (champs.length < 4) return;
            String[] t = champs[3].split(":");               // ["[14/Jun/2014","10","30","13"]
            if (t.length < 3) return;
            int h = Integer.parseInt(t[1]);
            int min = Integer.parseInt(t[2]);
            int quart = min / 15 * 15;                       // 44 -> 30 ; 45 -> 45
            int total = h * 60 + quart;
            if (total < DEBUT || total > FIN) return;
            context.write(new Text(String.format("%02d:%02d", h, quart)), UN);
        }
    }

    public static class HitsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int somme = 0;
            for (IntWritable v : values) somme += v.get();
            context.write(key, new IntWritable(somme));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) { System.out.println("Usage: HitsDriver <in> <out>"); System.exit(-1); }
        Job job = Job.getInstance();
        job.setJarByClass(HitsDriver.class);
        job.setJobName("Hits par quart d'heure");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(HitsMapper.class);
        job.setCombinerClass(HitsReducer.class);   // somme partielle côté Map : moins de données au shuffle
        job.setReducerClass(HitsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
