package exp2;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * 计数hashmap
 */
public class CountMap<K> extends HashMap<K, Integer> {

    private static final long serialVersionUID = -3844164299046338986L;

    public Integer add(K key) {
        return add(key, 1);
    }

    public Integer add(K key,
                       int add) {
        Integer v = get(key);
        v = v == null ? 1 : v + add;
        put(key, v);
        return v;
    }

    public void addAll(@SuppressWarnings("unchecked") K... keys) {
        if (keys != null) {
            for (K k : keys) {
                add(k);
            }
        }
    }

    public void addAll(Collection<K> keys) {
        if (CollectionUtils.isNotEmpty(keys)) {
            for (K k : keys) {
                add(k);
            }
        }
    }

    public void addAll(CountMap<K> map) {
        if (MapUtils.isNotEmpty(map)) {
            for (Entry<K, Integer> entry : map.entrySet()) {
                add(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * 对该countmap中每个值都进行提权
     */
    public CountMap<K> boost(int boost) {
        entrySet().stream().forEach(e -> e.setValue(e.getValue() + boost));
        return this;
    }

    /**
     * 对countmap排序并做截断
     *
     * @cutSize 要保留几个
     * @max cut最大的还是最小的
     */
    public CountMap<K> cut(int cutSize,
                           boolean max) {
        if (size() > cutSize) {
            ArrayList<Entry<K, Integer>> list = new ArrayList<Entry<K, Integer>>(entrySet());
            list.sort(new Comparator<Entry<K, Integer>>() {
                @Override
                public int compare(Entry<K, Integer> o1,
                                   Entry<K, Integer> o2) {
                    return (max ? -1 : 1) * (o1.getValue() - o2.getValue());
                }
            });
            for (int i = size() - 1; i >= cutSize; i--) {
                remove(list.get(i).getKey());
            }
        }
        return this;
    }

    /**
     * 输出排序的结果
     */
    public List<Map.Entry<K, Integer>> toSortedList(boolean desc) {
        ArrayList<Map.Entry<K, Integer>> result = new ArrayList<>();
        result.addAll(entrySet());
        result.sort(new Comparator<Entry<K, Integer>>() {
            @Override
            public int compare(Entry<K, Integer> o1, Entry<K, Integer> o2) {
                if (desc) {
                    return o2.getValue() - o1.getValue();
                } else {
                    return o1.getValue() - o2.getValue();
                }
            }
        });
        return result;
    }

    /**
     * @return 张三:1,李四:2,王五:3,...,xx:n
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (Entry<K, Integer> entry : entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
        }
        return sb.length() > 0 ? sb.substring(0, sb.length() - 1) : sb.toString();
    }

    /**
     * @return 张三:1,李四:2,王五:3,...,xx:n
     */
    public String toSortedString(boolean desc) {
        StringBuffer sb = new StringBuffer();
        for (Entry<K, Integer> entry : toSortedList(desc)) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
        }
        return sb.length() > 0 ? sb.substring(0, sb.length() - 1) : sb.toString();
    }

    /**
     * 通过字符串转为countmap，这里需要保证输入字符串格式一定正确，否则强制报错退出
     */
    public static CountMap<String> fromString(String input) {
        CountMap<String> rsMap = new CountMap<String>();
        if (StringUtils.isNotBlank(input)) {
            String[] toks = input.split("[,:]");
            for (int i = 0; i < toks.length; i += 2) {
                rsMap.put(toks[i], Integer.parseInt(toks[i + 1]));
            }
        }
        return rsMap;
    }

    public static void main(String[] args) {
        CountMap<String> map = new CountMap<>();
        map.add("a");
        map.add("a");
        map.add("a");
        map.add("b");
        map.add("b");
        map.add("c");
        map.add("b");
        map.add("a");
        map.add("a");
        map.add("c");
        map.add("c");
        map.add("c");
        map.add("c");
        map.add("c");
        map.add("c");
        System.out.println(map);
        System.out.println(map.toSortedList(true));
        System.out.println(map.toSortedList(false));
        System.out.println(map.toSortedString(true));
    }
}
