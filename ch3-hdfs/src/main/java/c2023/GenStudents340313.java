package c2023;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class GenStudents340313 {
    public static Random rand = new Random();
    public static String[] family_names;
    public static String[] common_words;
    public static String[] birthdays;
    public static String[] phone_starts;
    public static String[] cities;

    static {
        family_names = loadToStringArray("data/family_names.txt");
        common_words = loadToStringArray("data/common_words.txt");
        List<String> birthdayList = new ArrayList<String>(4 * 365);
        long from = System.currentTimeMillis() - 22l * 365 * 24 * 3600 * 1000;
        long to = System.currentTimeMillis() - 19l * 365 * 24 * 3600 * 1000;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        for (long t = from; t < to; t += 24l * 3600 * 1000) {
            birthdayList.add(sdf.format(new Date(t)));
        }
        birthdays = birthdayList.toArray(new String[birthdayList.size()]);
        phone_starts = loadToLineArray("data/phone_start.txt");
        cities = loadToLineArray("data/citys.txt");
    }
    public static String[] loadToLineArray(String path) {
        try {
            ArrayList<String> allwords = new ArrayList<>();
            BufferedReader br = new BufferedReader(new FileReader(path));
            String line = null;
            while ((line = br.readLine()) != null) {
                allwords.add(line.trim());
            }
            return allwords.toArray(new String[allwords.size()]);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public static String[] loadToStringArray(String path) {
        try {
            ArrayList<String> allwords = new ArrayList<>();
            BufferedReader br = new BufferedReader(new FileReader(path));
            String line = null;
            while ((line = br.readLine()) != null) {
                for (String word : line.trim().split("")) {
                    allwords.add(word);
                }
            }
            return allwords.toArray(new String[allwords.size()]);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public static String genNames() {
        StringBuilder sb = new StringBuilder();
        sb.append(family_names[rand.nextInt(family_names.length)]);
        sb.append(common_words[rand.nextInt(common_words.length)]);
        sb.append(common_words[rand.nextInt(common_words.length)]);
        return sb.toString();
    }
    public static void main(String[] args) throws Exception{
        String localFile = "data/s100.data";
        FileWriter fw = new FileWriter(localFile);
        for(int clazz=0;clazz<100;clazz++){
            String clazzID = (clazz+100+"").substring(1);
            for(int sid=0;sid<10000;sid++){
                String sidstr = (sid+10000+"").substring(1);
                StringBuilder sb = new StringBuilder();
                sb.append(genNames()).append("\t");
                sb.append("RB"+clazzID).append("\t");
                sb.append("RB"+clazzID+""+sidstr).append("\t");
                sb.append(birthdays[rand.nextInt(birthdays.length)]).append("\t");
                String phoneNum = (rand.nextInt(100000000)+100000000+"").substring(1);
                sb.append(phone_starts[rand.nextInt(phone_starts.length)]).append(phoneNum).append("\t");
                sb.append(cities[rand.nextInt(cities.length)]).append(rand.nextInt(100)).append("号\t");
                sb.append(rand.nextInt(100)).append("\n");
                fw.write(sb.toString());
//                System.out.println(sb);
            }
        }
        fw.close();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://zzti:9000"), conf, "zzti");
        fs.copyFromLocalFile(new Path(localFile), new Path("/"));

        fs.close();
    }
}
