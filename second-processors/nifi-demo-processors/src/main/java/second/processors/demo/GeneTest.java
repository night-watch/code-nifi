package second.processors.demo;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.*;

/**
 * Created by NightWatch on 2018/2/1.
 */
public class GeneTest extends AbstractProcessor {



    public static final Relationship MY_RELATIONSHIP_SUCEESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Example relationship")
            .build();

    private Set<Relationship> relationships;


    @Override
    protected void init(ProcessorInitializationContext context) {

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP_SUCEESS);
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        JSONArray jsonArray = new JSONArray();

        String userId = forUserId();


        int i1 = (int) (Math.random() * 40) + 1;

        for (int i = 0; i < i1; i++) {
            JSONObject jsonObject = new JSONObject();

            jsonObject.put("user_id", userId);

            jsonObject.put("request_data_num", "100");

            jsonArray.add(jsonObject);
        }

        FlowFile flowFile = session.create();

        FlowFile write = session.write(flowFile, out -> {
            out.write(jsonArray.toString().getBytes());
        });
        session.transfer(write, MY_RELATIONSHIP_SUCEESS);
    }


    private String forUserId() {
        String[] chars = {"a", "b", "c", "d", "e", "f", "g", "h", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0"};
        StringBuffer userId = new StringBuffer();
        for (int i = 0; i < 32; i ++) {
            Integer index = new Double(Math.random()*18).intValue();
            userId.append(chars[index]);
        }
        return userId.toString();
    }
}
