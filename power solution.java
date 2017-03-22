 import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Solution
{
      
public static class rahul extends Mapper<Object, Text, Text, IntWritable>
{
private final static IntWritable one = new IntWritable(1);
private static Text word = new Text();
static String k="";
 int e=0,p,q;
 float a[][],r[],g[],b[][];
Text t=new Text();
public void map(Object a1,Text b1,Context c)throws IOException, InterruptedException
{
String s2=b1.toString();
String s1[]=s2.split(" ");
int s=Integer.parseInt(s1[0]);
int t=Integer.parseInt(s1[1]);
a=new float[t][s];
b=new float[t][s];
r=new float[t];
g=new float[t];

e=2;
for(p=0;p<t;p++)
{
for(q=0;q<s;q++)
{
a[p][q]=Float.parseFloat(s1[e]);
e++;
}
}
for(int i=0;i<t;i++)
{
for(int j=0;j<s;j++)
{

r[i]=r[i]+a[i][j];
//r[i]+=temp;
}
//System.out.println(r[i]);
}
for(int i=0;i<t;i++)
{
for(int j=0;j<s;j++)
{
a[i][j]=a[i][j]/r[i];
}
}
for(int i=0;i<t;i++)
{
for(int j=0;j<s;j++)
{
a[i][j]=1-a[i][j];
//System.out.println(a[i][j]);
}
}
for(int i=e,j=0;i<s1.length;i++,j++)
{
g[j]=Float.parseFloat(s1[i]);
}
for(int i=0;i<t;i++)
{
for(int j=0;j<s;j++)
{
//System.out.println(g[i]);
//System.out.println(a[i][j]);
b[i][j]=a[i][j]*g[i];
k+=" "+b[i][j]+" ";
word.set(k+" "+r[0]+" "+s1.length);
//c.write(new Text(k),new IntWritable(1));
//System.out.println(b[i][j]);
}c.write(word,one);
}

}

}
/*private class karthik extends reducer<Text,Intwritable,Text,Intwritable>
{
int sum;
IntWritable n=new IntWritable();
public void reduce(Text x,IntWritable<iterable> y,Context z)
{
for(IntWritable v1:y)
{
sum+=v1.get();
}
z.write(x,n.set(sum))	
}
}*/
public static void main (String[] args)throws Exception
{
Configuration c=new Configuration();
c.addResource(new Path("/home/hadoop/Desktop/hadoop/conf/core-site.xml"));
c.addResource(new Path("/home/hadoop/Desktop/hadoop/conf/hdfs-site.xml"));
c.addResource(new Path("/home/hadoop/Desktop/hadoop/conf/mapred-site.xml"));
Job j=new Job(c,"snarc");
j.setJarByClass(Solution.class);
j.setMapperClass(rahul.class);
//j.setCombinerClass("karthik.class");
//j.setReducerClass("karthik.class");
FileInputFormat.addInputPath(j,new Path(args[0]));
FileOutputFormat.setOutputPath(j,new Path(args[1]));
j.setOutputKeyClass(Text.class);
j.setOutputValueClass(IntWritable.class);
j.waitForCompletion(true);
}
}

