package cn.wchy.spark.day8.tags;


import cn.wchy.spark.day8.ahocorasick2.AhoCorasick;
import cn.wchy.spark.day8.ahocorasick2.SearchResult;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

public class AcSearch {
    public static String searchValue(AhoCorasick ac, String name) {
        try {
            Iterator<SearchResult> iter = ac.search(name.getBytes());
            SearchResult result = null;
            if (iter.hasNext()) {
                result = iter.next();
                Set<String> sid = result.getOutputs();
                if (!sid.isEmpty()) {
                    Object[] array = sid.toArray();
                    Arrays.sort(array);
                    return array[array.length - 1].toString();
                }
            }
            return "0";
        } catch (Exception e) {
            return "0";
        }
    }
}
