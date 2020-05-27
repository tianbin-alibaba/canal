package com.alibaba.otter.canal.connector.core.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

/**
 * 基于aviater进行tableName正则匹配的过滤算法
 *
 * @author jianghang 2012-7-20 下午06:01:34
 */
public class AviaterRegexFilter {
    //我们的配置的binlog过滤规则可以由多个正则表达式组成，使用逗号”,"进行分割
    private static final String             SPLIT             = ",";
    //将经过逗号",”分割后的过滤规则重新使用|串联起来
    private static final String             PATTERN_SPLIT     = "|";
    //canal定义的Aviator过滤表达式，使用了regex自定义函数，接受pattern和target两个参数
    private static final String             FILTER_EXPRESSION = "regex(pattern,target)";
    //regex自定义函数实现，RegexFunction的getName方法返回regex，call方法接受两个参数
    private static final RegexFunction      regexFunction     = new RegexFunction();
    //对自定义表达式进行编译，得到Expression对象
    private final Expression                exp               = AviatorEvaluator.compile(FILTER_EXPRESSION, true);
    static {
        //将自定义函数添加到AviatorEvaluator中
        AviatorEvaluator.addFunction(regexFunction);
    }
    //用于比较两个字符串的大小
    private static final Comparator<String> COMPARATOR        = new StringComparator();
    //用户设置的过滤规则，需要使用SPLIT进行分割
    final private String                    pattern;
    //在没有指定过滤规则pattern情况下的默认值，例如默认为true，表示用户不指定过滤规则情况下，总是返回所有的binlog event
    final private boolean                   defaultEmptyValue;

    public AviaterRegexFilter(String pattern){
        this(pattern, true);
    }
    //构造方法
    public AviaterRegexFilter(String pattern, boolean defaultEmptyValue){
        //1 给defaultEmptyValue字段赋值
        this.defaultEmptyValue = defaultEmptyValue;
         //2、给pattern字段赋值
         //2.1 将传入pattern以逗号",”进行分割，放到list中；如果没有指定pattern，则list为空，意味着不需要过滤
        List<String> list = null;
        if (StringUtils.isEmpty(pattern)) {
            list = new ArrayList<String>();
        } else {
            String[] ss = StringUtils.split(pattern, SPLIT);
            list = Arrays.asList(ss);
        }

        // 对pattern按照从长到短的排序
        // 因为 foo|foot 匹配 foot 会出错，原因是 foot 匹配了 foo 之后，会返回 foo，但是 foo 的长度和 foot
        // 的长度不一样
        Collections.sort(list, COMPARATOR);
        // 对pattern进行头尾完全匹配
        list = completionPattern(list);
        //2.4 将过滤规则重新使用|串联起来赋值给pattern
        this.pattern = StringUtils.join(list, PATTERN_SPLIT);
    }

    public boolean filter(String filtered) {
        if (StringUtils.isEmpty(pattern)) {
            return defaultEmptyValue;
        }

        if (StringUtils.isEmpty(filtered)) {
            return defaultEmptyValue;
        }

        Map<String, Object> env = new HashMap<String, Object>();
        env.put("pattern", pattern);
        env.put("target", filtered.toLowerCase());
        return (Boolean) exp.execute(env);
    }

    /**
     * 修复正则表达式匹配的问题，因为使用了 oro 的 matches，会出现：
     *
     * <pre>
     * foo|foot 匹配 foot 出错，原因是 foot 匹配了 foo 之后，会返回 foo，但是 foo 的长度和 foot 的长度不一样
     * </pre>
     *
     * 因此此类对正则表达式进行了从长到短的排序
     *
     * @author zebin.xuzb 2012-10-22 下午2:02:26
     * @version 1.0.0
     */
    private static class StringComparator implements Comparator<String> {

        @Override
        public int compare(String str1, String str2) {
            if (str1.length() > str2.length()) {
                return -1;
            } else if (str1.length() < str2.length()) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    /**
     * 修复正则表达式匹配的问题，即使按照长度递减排序，还是会出现以下问题：
     *
     * <pre>
     * foooo|f.*t 匹配 fooooot 出错，原因是 fooooot 匹配了 foooo 之后，会将 fooo 和数据进行匹配，但是 foooo 的长度和 fooooot 的长度不一样
     * </pre>
     *
     * 因此此类对正则表达式进行头尾完全匹配
     *
     * @author simon
     * @version 1.0.0
     */

    private List<String> completionPattern(List<String> patterns) {
        List<String> result = new ArrayList<String>();
        for (String pattern : patterns) {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("^");
            stringBuffer.append(pattern);
            stringBuffer.append("$");
            result.add(stringBuffer.toString());
        }
        return result;
    }

    @Override
    public String toString() {
        return pattern;
    }

}
