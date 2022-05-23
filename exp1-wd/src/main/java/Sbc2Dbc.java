public class Sbc2Dbc {
    public static void main(String[] args) {
        String x1 = "~!@#$%^&*()_+{}|:\"<>?[]\\;',./'";
        String x2 = "~！@#￥%……&*（）——+{}|：“《》？【】、；‘，。、";
        System.out.println(x1);
        System.out.println(ToSBC(x1));
        System.out.println(ToDBC(x1));
        System.out.println(x2);
        System.out.println(ToSBC(x2));
        System.out.println(ToDBC(x2));
        System.out.println('a' + 0);
        System.out.println('z' + 0);
        System.out.println('0' + 0);
        System.out.println('9' + 0);
    }

    /**
     * 转全角(SBC case)
     * 全角空格为12288,半角空格为32，其他字符半角(33-126)与全角(65281-65374)的对应关系是：均相差65248
     */
    public static String ToSBC(String input) { // 半角转全角：
        char[] c = input.toCharArray();
        for (int i = 0; i < c.length; i++) {
            if (c[i] == 32) {
                c[i] = (char) 12288;
                continue;
            }
            if (c[i] < 127)
                c[i] = (char) (c[i] + 65248);
        }
        return new String(c);
    }

    /**
     * 转全角但忽略字母、数字、空格
     */
    public static String ToSBCWithoutLetterNumSpace(String input) { // 半角转全角：
        char[] c = input.toCharArray();
        for (int i = 0; i < c.length; i++) {
            if (c[i] == 32 || (c[i] >= 'a' + 0 || c[i] <= 'z' + 0) || ((c[i] >= '0' + 0 || c[i] <= '9' + 0))) {
                continue;
            }
            if (c[i] < 127)
                c[i] = (char) (c[i] + 65248);
        }
        return new String(c);
    }

    /**
     * 转半角(DBC case)
     * 全角空格为12288，半角空格为32，其他字符半角(33-126)与全角(65281-65374)的对应关系是：均相差65248
     */
    public static String ToDBC(String input) {
        char[] c = input.toCharArray();
        for (int i = 0; i < c.length; i++) {
            if (c[i] == 12288) {
                c[i] = (char) 32;
                continue;
            }
            if (c[i] > 65280 && c[i] < 65375)
                c[i] = (char) (c[i] - 65248);
        }
        return new String(c);
    }
}
