package ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 构建100W的学生账号并导入HDFS
 */
public class GenStudents {
    private static Random rand = new Random();
    private static int clazzLength = 1000; // 有1000个班，大概1G
    private static char[] family_names;
    private static char[] common_words;
    private static String[] clazz;
    private static String[] gender = { "男", "女" };
    private static String[] birthday;
    private static String[] phone;
    private static String[] city;

    static {
        try {
            // 加载百家姓
            family_names = loadFileToString("data/family_names.txt").toCharArray();
            // 加载常用字
            common_words = loadFileToString("data/common_words.txt").toCharArray();
            // 生成班级，学号不用生成，每1W人从0开始计数并写入即可
            clazz = new String[clazzLength];
            for (int i = 0; i < clazzLength; i++) {
                clazz[i] = "RB" + String.format("%03d", i);
            }
            // 生成出生年月日，截取3年的
            List<String> birthdayList = new ArrayList<String>(4 * 365);
            long from = System.currentTimeMillis() - 22l * 365 * 24 * 3600 * 1000;
            long to = System.currentTimeMillis() - 19l * 365 * 24 * 3600 * 1000;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            for (long t = from; t < to; t += 24l * 3600 * 1000) {
                birthdayList.add(sdf.format(new Date(t)));
            }
            birthday = birthdayList.toArray(new String[birthdayList.size()]);
            // 加载常用电话号段（前3位，后8位随机生成）
            phone = loadFileToArr("data/phone_start.txt");
            // 生成省份和地市，门牌号随机生成
            city = loadFileToArr("data/citys.txt");

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static String loadFileToString(String filename) throws Exception {
        StringBuilder sb = new StringBuilder();
        BufferedReader br = new BufferedReader(new FileReader(filename));
        while (br.ready()) {
            sb.append(br.readLine().trim());
        }
        br.close();
        return sb.toString();
    }

    public static String[] loadFileToArr(String filename) throws Exception {
        List<String> arrList = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(filename));
        while (br.ready()) {
            String line = br.readLine().trim();
            if (line.length() > 0) {
                arrList.add(line);
            }
        }
        br.close();
        return arrList.toArray(new String[arrList.size()]);
    }

    // 随机生成名字
    public static String genRandName() {
        StringBuilder sb = new StringBuilder();
        sb.append(family_names[rand.nextInt(family_names.length)]);
        sb.append(common_words[rand.nextInt(common_words.length)]);
        sb.append(common_words[rand.nextInt(common_words.length)]);
        return sb.toString();
    }

    // 随机生成电话
    public static String genRandPhone() {
        return genRandArr(phone) + String.format("%08d", rand.nextInt(100000000));
    }

    // 随机生成地址
    public static String genLoc() {
        return genRandArr(city) + String.format("%03d", rand.nextInt(1000)) + "号";
    }

    // 随机生成地址
    public static int genScore() {
        return rand.nextInt(100);
    }

    public static String genRandArr(String[] arr) {
        return arr[rand.nextInt(arr.length)];
    }

    public static String genStuLine(String stuClazz, int num) {
        StringBuilder sb = new StringBuilder();
        sb.append(genRandName()).append("\t");
        sb.append(stuClazz).append("\t");
        sb.append(stuClazz + String.format("%04d", num)).append("\t");
        sb.append(genRandArr(gender)).append("\t");
        sb.append(genRandArr(birthday)).append("\t");
        sb.append(genRandPhone()).append("\t");
        sb.append(genLoc()).append("\t");
        sb.append(genScore()).append("\n");
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        // 生成数据
        String filepath = "C:\\nos\\4-my-hadoop\\test\\students_"+clazzLength+"w.data";
        FileWriter fw = new FileWriter(filepath);
        for (int i = 0; i < clazzLength * 10000; i++) {
            String line = genStuLine(clazz[i / 10000], i % 10000);
            fw.write(line);
        }
        fw.close();
        // 上传文件
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path from = new Path(filepath);
        Path to = new Path("/ch3");
        fs.mkdirs(to);
        fs.copyFromLocalFile(from, to);
        fs.close();
    }
}
