package exo;                                    // = nom du dossier (exo/ExoDriver.java)

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ExoDriver {

    // ================= MAPPER : appelé 1 fois par LIGNE, émet 0, 1 ou n couples (clé, valeur)
    //                 entrée (ne pas toucher)  | sortie Map : <K_MAP, V_MAP>
    public static class ExoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private String param;                                     // 3e argument de la commande (ou null)

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            param = context.getConfiguration().get("param");      // lu 1 fois, pas à chaque ligne
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String ligne = value.toString().trim();               // trim() enlève aussi le \r Windows
            if (ligne.isEmpty()) return;                          // ligne vide : ignorée
            // if (key.get() == 0) return;                        // CSV avec en-tête : saute la 1re ligne

            String[] champs = ligne.split(";");                   // ";" CSV | "\\s+" mots | "\t" après cat -n
            // if (champs.length < 3) return;                     // ligne incomplète : ignorée

            // TODO 1 — FILTRE : return si la ligne ne m'intéresse pas
            // TODO 2 — CLÉ = ce sur quoi on regroupe ; VALEUR = ce qu'on agrège
            String cle = champs[0];
            context.write(new Text(cle), new IntWritable(1));
        }
    }

    // ================= REDUCER : appelé 1 fois par CLÉ, avec TOUTES ses valeurs
    //                   entrée = sortie Map         | sortie finale : <K_RED, V_RED>
    public static class ExoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int somme = 0;
            for (IntWritable v : values) {
                somme += v.get();                                 // v.get() : IntWritable -> int
            }
            // TODO 3 — filtre sur le résultat (ex. if (somme < 2) return;)
            context.write(key, new IntWritable(somme));
        }
    }

    // ================= DRIVER : configure et lance le job
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage : ExoDriver <rep_entree> <rep_sortie> [param]");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        if (args.length > 2) job.getConfiguration().set("param", args[2]);   // transmis au Mapper

        job.setJarByClass(ExoDriver.class);
        job.setJobName("Exo");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));          // ne doit PAS exister

        job.setMapperClass(ExoMapper.class);
        job.setReducerClass(ExoReducer.class);
        // job.setCombinerClass(ExoReducer.class);  // si somme/max ET sortie Reducer = sortie Map
        // job.setNumReduceTasks(1);                 // Top-N global : un seul Reducer
        // job.setNumReduceTasks(0);                 // map-only (filtre) : sortie part-m-00000

        job.setMapOutputKeyClass(Text.class);           // = K_MAP
        job.setMapOutputValueClass(IntWritable.class);  // = V_MAP
        job.setOutputKeyClass(Text.class);              // = K_RED
        job.setOutputValueClass(IntWritable.class);     // = V_RED

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
