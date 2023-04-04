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

public class GenStudents0313 {
    public static Random rand = new Random();
    public static String[] family_names;
    public static String[] common_words;
    public static String[] genders={"男","女"};
    public static String[] birthdays;
    public static String[] phone_starts;
    public static String[] citys;
    static {
        String family_names_str = loadFileToString("data/family_names.txt");
        family_names = family_names_str.split("");
        String common_words_str = loadFileToString("data/common_words.txt");
        common_words = common_words_str.split("");
        List<String> birthdayList = new ArrayList<String>(4 * 365);
        long from = System.currentTimeMillis() - 22l * 365 * 24 * 3600 * 1000;
        long to = System.currentTimeMillis() - 19l * 365 * 24 * 3600 * 1000;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        for (long t = from; t < to; t += 24l * 3600 * 1000) {
            birthdayList.add(sdf.format(new Date(t)));
        }
        birthdays = birthdayList.toArray(new String[birthdayList.size()]);
        phone_starts = loadFileToArray("data/phone_start.txt");
        citys = loadFileToArray("data/citys.txt");
    }
    public static String loadFileToString(String path) {
        try {
            StringBuilder sb = new StringBuilder();
            BufferedReader br = new BufferedReader(new FileReader(path));
            String line = null;
            while ((line = br.readLine()) != null) {
                sb.append(line.trim());
            }
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public static String[] loadFileToArray(String path){
        try {
            ArrayList<String> phone_list = new ArrayList<>();
            BufferedReader br = new BufferedReader(new FileReader(path));
            String line = null;
            while ((line = br.readLine()) != null) {
                phone_list.add(line.trim());
            }
            return phone_list.toArray(new String[phone_list.size()]);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public static String getName() {
        StringBuilder sb = new StringBuilder();
        sb.append(family_names[rand.nextInt(family_names.length)]);
        sb.append(common_words[rand.nextInt(common_words.length)]);
        sb.append(common_words[rand.nextInt(common_words.length)]);
        return sb.toString();
    }
    public static String getPhone(){
        StringBuilder sb = new StringBuilder();
        sb.append(phone_starts[rand.nextInt(phone_starts.length)]);
        String phone_last = (rand.nextInt(100000000)+100000000+"").substring(1);
        sb.append(phone_last);
        return sb.toString();
    }
    public static void main(String[] args) throws Exception {
        String local_students_path = "data/students_100w.data";
        FileWriter fw = new FileWriter(local_students_path);
        for(int clazz=0;clazz<100;clazz++){
            String clazz_str = (clazz+100+"").substring(1);
            for(int sid=0;sid<10000;sid++){
                String sid_str = (sid+10000+"").substring(1);

                StringBuilder sb = new StringBuilder();
                sb.append(getName()).append("\t");
                sb.append("RB"+clazz_str).append("\t");
                sb.append("RB"+clazz_str+sid_str).append("\t");
                sb.append(genders[rand.nextInt(2)]).append("\t");
                sb.append(birthdays[rand.nextInt(birthdays.length)]).append("\t");
                sb.append(getPhone()).append("\t");
                sb.append(citys[rand.nextInt(citys.length)])
                        .append(rand.nextInt(1000)).append("号\t");
                sb.append(rand.nextInt(100)).append("\n");
//                System.out.println(sb);
                fw.write(sb.toString());
            }
        }
        fw.close();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://zzti:9000"), conf, "zzti");
        fs.copyFromLocalFile(new Path(local_students_path),new Path("/"));
        fs.close();
    }
}
