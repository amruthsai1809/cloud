import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;

public class pagerank extends Configured implements Tool {

    //patterns for title and url
    public static final String TitlePattern = "(<title>)([\\s\\S]*?)(</title>)", URLPattern = "(\\[\\[)([\\s\\S]*?)(\\]\\])";
    public static void main( String[] args) throws  Exception {
        int res  = ToolRunner .run( new pagerank(), args);
        System .exit(res);
    }

    public int run( String[] args) throws  Exception {

        //this job generates the link graph
        Job job  = Job .getInstance(getConf(), " pagerank ");
        job.setJarByClass( this .getClass());
        FileInputFormat.addInputPaths(job,  args[0]);
        FileOutputFormat.setOutputPath(job,  new Path("intermediate/graph"));
        job.setMapperClass( Map .class);
        job.setReducerClass( Reduce .class);
        job.setOutputKeyClass( Text .class);
        job.setOutputValueClass( Text .class);
        job.waitForCompletion( true);


        //this job calculates the page rank for each node after ten iterations
        Job job2;
        for(int i=0; i<10; i++){
            job2 = Job .getInstance(getConf(), " pagerank ");
            job2.setJarByClass( this .getClass());
            if(i==0){
                FileInputFormat.addInputPaths(job2,  "intermediate/graph");
            }else{
                FileInputFormat.addInputPaths(job2,  "intermediate/pagerank"+i);
            }
            FileOutputFormat.setOutputPath(job2,  new Path("intermediate/pagerank"+(i+1)));
            job2.setMapperClass( Map2 .class);
            job2.setReducerClass( Reduce2 .class);
            job2.setOutputKeyClass( Text .class);
            job2.setOutputValueClass( Text .class);
            job2.waitForCompletion( true);
        }


        // this job deletes the intermediate folder and writes page ranks of all the nodes in descending order
        Job job3  = Job .getInstance(getConf(), " pagerank ");
        job3.setJarByClass( this .getClass());
        job3.setNumReduceTasks(1);
        FileInputFormat.addInputPaths(job3,  "intermediate/pagerank10");
        FileOutputFormat.setOutputPath(job3,  new Path(args[ 1]));
        job3.setMapperClass( Map3 .class);
        job3.setReducerClass( Reduce3 .class);
        job3.setOutputKeyClass( DoubleWritable .class);
        job3.setOutputValueClass( Text .class);

        job3.waitForCompletion( true);


        // this code deletes the intermediate folder which is no longer necessary
        Configuration configuration = getConf();
        Path path =  new Path("intermediate");
        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(path)){
            fs.delete(path, true);
        }
        return 1;

    }

    public static class Map extends Mapper<LongWritable ,  Text , Text, Text > {
        public void map( LongWritable nouse,  Text line,  Context context)
                throws  IOException,  InterruptedException {


            //creating pattern and matcher objects for url
            Pattern url = Pattern.compile(URLPattern);
            java.util.regex.Matcher urlmatcher = url.matcher(line.toString());


            //creating pattern and matcher objects for title
            Pattern title = Pattern.compile(TitlePattern);
            java.util.regex.Matcher titleMatcher = title.matcher(line.toString());

            String linkNames = "";
            while (urlmatcher.find( )) {
                String urlm = urlmatcher.group(2);
                linkNames = linkNames+urlm+"#####";
            }

            while (titleMatcher.find( )) {
                String match = titleMatcher.group(2);
                if(!(linkNames.length()>0)){
                    String textFill = match+">>>"+linkNames;
                    context.write(new Text("count"), new Text(textFill));
                }
                else{
                    String textFill = match+">>>"+linkNames.substring(0,linkNames.length()-5);
                    context.write(new Text("count"), new Text(textFill));

                }
            }
        }
    }

    public static class Reduce extends Reducer< Text , Text,  Text ,  DoubleWritable > {
        @Override
        public void reduce( Text count,  Iterable<Text > namesandlinks,  Context context)
                throws IOException,  InterruptedException {
            ArrayList<String> te = new ArrayList<>();
            Double numberOfLinks = 0.0;

            // calculation the number of links
            for(Text temp: namesandlinks){
                te.add(temp.toString());
                numberOfLinks = numberOfLinks + 1;
            }

            // giving the same page rank for all the nodes
            Double samePageRank = (double) (1/numberOfLinks);
            for (int i=0; i<te.size();i++){
                context.write(new Text(te.get(i)),new DoubleWritable(samePageRank));
            }
        }
    }

    // page rank for each link is calculated in this mapper
    public static class Map2 extends Mapper<LongWritable ,  Text , Text, Text > {
        public void map( LongWritable nouse,  Text line,  Context context)
                throws  IOException,  InterruptedException {
            String[] pageAndItsLinks = line.toString().split("\\t")[0].split(">>>");
            Double linkPageRank = Double.parseDouble(line.toString().split("\\t")[1]);
            if(pageAndItsLinks.length>=2){
                String[] links = pageAndItsLinks[1].split("#####");
                context.write(new Text(pageAndItsLinks[0]), new Text("pointingTo"+pageAndItsLinks[1]));
                for(String link: links){
                    Double fraction = linkPageRank/links.length;
                    context.write(new Text(link), new Text(fraction.toString()));
                }
            }

        }
    }

    // summation of all the page rank fraction for each link is done here and this is done repeatedly for 10 times
    public static class Reduce2 extends Reducer< Text , Text,  Text ,  Text > {
        @Override
        public void reduce( Text nodeName,  Iterable<Text > pageRank,  Context context)
                throws IOException,  InterruptedException {

            String page = nodeName.toString(),outGoingLinks = "";
            Double newPageRank = 0.0;
            for(Text temp: pageRank){
                if(!(temp.toString().contains("pointingTo"))){
                    newPageRank += Double.parseDouble(temp.toString());
                }else{
                    outGoingLinks = temp.toString().substring(10, temp.toString().length());
                }
            }
            newPageRank = 0.15+0.85*(newPageRank);
            String pageRankString = String.valueOf(newPageRank);
            if(!(outGoingLinks.equals(""))){
                context.write(new Text(page+">>>"+outGoingLinks), new Text(pageRankString));
            }else{
                context.write(new Text(page), new Text(pageRankString));
            }

        }
    }


    // keys and values are swapped and negative values are give to all the keys
    public static class Map3 extends Mapper<LongWritable ,  Text , DoubleWritable, Text > {
        public void map( LongWritable nouse,  Text line,  Context context)
                throws  IOException,  InterruptedException {
            String[] pagesAndRank = line.toString().split("\\t");
            String page = pagesAndRank[0].split(">>>")[0];
            Double pageRankINV = -1.0*Double.parseDouble(pagesAndRank[1]);
            context.write(new DoubleWritable(pageRankINV), new Text(page));

        }
    }

    // key and values are swapped and negative values are changed to positive values and written to the file
    public static class Reduce3 extends Reducer< DoubleWritable , Text,  Text ,  Text > {
        @Override
        public void reduce( DoubleWritable count,  Iterable<Text > pages,  Context context)
                throws IOException,  InterruptedException {

            for(Text temp: pages){
                context.write(new Text(temp.toString()), new Text(-(count.get())+""));
            }

        }
    }

}
