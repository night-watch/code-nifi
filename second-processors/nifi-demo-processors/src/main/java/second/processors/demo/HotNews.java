package second.processors.demo;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import nifi.bicon.util.BigMap;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.*;

/**
 * Created by NightWatch on 2018/3/20.
 */
public class HotNews extends AbstractProcessor {
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success").description("success").build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure").description("failure").build();

    // 关系集合
    private Set<Relationship> relationships;

    // 自定义属性值列表
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(ProcessorInitializationContext context) {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
        this.descriptors = new ArrayList<>();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    public List<PropertyDescriptor> getDescriptors() {
        return this.descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            List<Map<String, String>> list = BigMap.newsTodayList.subList(0, 30);
            list.addAll(BigMap.newsOldList.subList(0, 30));
            list.sort(new Comparator<Map<String, String>>() {
                @Override
                public int compare(Map<String, String> m1, Map<String, String> m2) {
                    int readCount1 = Integer.parseInt(m1.get("read_count"));
                    int commentCount1 = Integer.parseInt(m1.get("comment_count"));
                    int collectionCount1 = Integer.parseInt(m1.get("collection_count"));
                    int readCount2 = Integer.parseInt(m2.get("read_count"));
                    int commentCount2 = Integer.parseInt(m2.get("comment_count"));
                    int collectionCount2 = Integer.parseInt(m2.get("collection_count"));
                    int count1 = readCount1 + commentCount1 * 2 + collectionCount1 * 3;
                    int count2 = readCount2 + commentCount2 * 2 + collectionCount2 * 3;
                    if (count1 > count2) {
                        return -1;
                    } else if (count1 == count2) {
                        return 0;
                    }
                    return 1;
                }
            });
            List<Map<String, String>> listReturn = list.subList(0, 30);
            JSONArray j = JSONArray.fromObject(listReturn);
            JSONObject back = new JSONObject();
            back.put("response_code", "00000000");
            back.put("response_msgs", "成功");
            back.put("response_time", new Date().getTime());
            back.put("type", "2");
            back.put("data", j);
            FlowFile write = session.write(flowFile, out -> {
                out.write(back.toString().getBytes());
            });
            session.transfer(write, SUCCESS);
        } catch (Exception e) {
            getLogger().error("HotNews", e);
            JSONObject back = new JSONObject();
            back.put("response_code", "00000001");
            back.put("response_msgs", "失败");
            back.put("response_time", new Date().getTime());
            back.put("type", "2");
            back.put("data", new JSONArray());
            FlowFile write = session.write(flowFile, out -> {
                out.write(back.toString().getBytes());
            });
            session.transfer(write, FAILURE);
        }
    }
}
