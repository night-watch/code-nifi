package second.processors.demo;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.collections.map.HashedMap;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by NightWatch on 2017/11/10.
 */

@Tags({"data", "test"})
@CapabilityDescription("根据用户输入的值，生成相应数量的测试数据(报文)")
public class ForData extends AbstractSessionFactoryProcessor {

    static final PropertyDescriptor COUNT = new PropertyDescriptor.Builder()
            .name("count")
            .description("要生成的测试数据的数量")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("成功")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("失败")
            .build();

    private static Integer COUNTS = -1;

    private static final Map<String, String> relation = new HashedMap();

    private static final List<Map<String, String>> listMap = new ArrayList<>();

    @Override
    protected void init(ProcessorInitializationContext context) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection mySQLConnect = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/life_health_data", "root", "123456");
            Statement mySQLStatement = mySQLConnect.createStatement();
            String sql = "select s.dictionary_code,s.small_value,s.big_value,d.dictionary_name from dictionary d,standard_data s where s.dictionary_code=d.dictionary_code and s.logic_delete=0 and d.logic_delete=0";
            ResultSet resultSet = mySQLStatement.executeQuery(sql);
            while (resultSet.next()) {
                Map<String, String> map = new HashedMap();
                map.put("dictionary_code", resultSet.getString(1));
                map.put("small_value", resultSet.getString(2));
                map.put("big_value", resultSet.getString(3));
                map.put("dictionary_name", resultSet.getString(4));
                String[] dictionaryCodes = resultSet.getString(1).split("_");
                String codeUnit = dictionaryCodes[0] + "_" + dictionaryCodes[1] + "_" + dictionaryCodes[2] + "_UNIT";
                String sqlUnit = "select dictionary_name from dictionary where dictionary_code = '" + codeUnit + "'";
                Statement mySQLStatement2 = mySQLConnect.createStatement();
                ResultSet rSetUnit = mySQLStatement2.executeQuery(sqlUnit);
                while (rSetUnit.next()) {
                    map.put("indexUnit", rSetUnit.getString(1));
                }
                listMap.add(map);
            }
            resultSet.close();
            mySQLStatement.close();
            mySQLConnect.close();
        } catch (Exception e) {
            getLogger().error("AAAAAAAAAAAAAAAAAAA0000000000000000000000forMap" + e);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
        propertyDescriptors.add(COUNT);
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @OnScheduled
    public void onScheduled() {
        relation.put("LRTZ_JKZB_TW", "BZZB_TZ_TW");
        relation.put("LRTZ_JKZB_XT_FQ", "BZZB_YL_XT_FQ");
        relation.put("LRTZ_JKZB_XT_FH", "BZZB_YL_XT_FH");
        relation.put("LRTZ_JKZB_XT_SJQ", "BZZB_YL_XT_SJQ");
        relation.put("LRTZ_JKZB_XL", "BZZB_YL_XL");
        relation.put("LRTZ_JKZB_XY_SSY", "BZZB_YL_XY_SSY");
        relation.put("LRTZ_JKZB_XY_SZY", "BZZB_YL_XY_SZY");
        relation.put("LRTZ_JKZB_XY1_YB", "BZZB_YL_XY1_YB");
        relation.put("LRTZ_JKZB_XY1_QDDL", "BZZB_YL_XY1_QDDL");
        relation.put("LRTZ_JKZB_XY1_ZDDL", "BZZB_YL_XY1_ZDDL");
        relation.put("LRTZ_JKZB_DGC", "BZZB_YL_DGC");
        relation.put("LRTZ_JKZB_HXPL", "BZZB_YL_HXPL");
        relation.put("LRTZ_YSRL_ZC", "BZZB_RC_ZC");
        relation.put("LRTZ_YSRL_WC", "BZZB_RC_WC");
        relation.put("LRTZ_YSRL_WC1", "BZZB_RC_WC1");
        relation.put("LRTZ_RCXX_HS", "BZZB_RC_HS");
        relation.put("LRTZ_RCXX_SMSJ", "BZZB_RC_SMSJ");
        relation.put("LRTZ_RCXX_WXSJ", "BZZB_RC_WXSJ");
        relation.put("LRTZ_RCXX_ZLSJ", "BZZB_RC_ZLSJ");
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory processSessionFactory) throws ProcessException {

        ProcessSession session = processSessionFactory.createSession();
        FlowFile flowFile = session.create();
        Integer count = context.getProperty(COUNT).asInteger();
        final JSONObject jsonObject = json();
        FlowFile flowFileWrite = null;
        session.putAttribute(flowFile, "count", String.valueOf(count));
        try {
            flowFileWrite = session.write(flowFile, out -> {
                out.write(jsonObject.toString().getBytes());
            });
            session.transfer(flowFileWrite, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("AAAAAAAAAAAAAAAAAAAAAAAA000000000000000000000000write" + e);
            session.transfer(flowFile, REL_FAILURE);
        }
        session.commit();
    }

    private JSONObject json() {
        List<String> list = new ArrayList<>();
        list.add("LRTZ_JKZB_TW");
        list.add("LRTZ_JKZB_XT_FQ");
        list.add("LRTZ_JKZB_XT_FH");
        list.add("LRTZ_JKZB_XT_SJQ");
        list.add("LRTZ_JKZB_XL");
        list.add("LRTZ_JKZB_XY_SSY");
        list.add("LRTZ_JKZB_XY_SZY");
        list.add("LRTZ_JKZB_XY1_YB");
        list.add("LRTZ_JKZB_XY1_QDDL");
        list.add("LRTZ_JKZB_XY1_ZDDL");
        list.add("LRTZ_JKZB_DGC");
        list.add("LRTZ_JKZB_HXPL");
        list.add("LRTZ_YSRL_ZC");
        list.add("LRTZ_YSRL_WC");
        list.add("LRTZ_YSRL_WC1");
        list.add("LRTZ_RCXX_HS");
        list.add("LRTZ_RCXX_SMSJ");
        list.add("LRTZ_RCXX_WXSJ");
        list.add("LRTZ_RCXX_ZLSJ");
        Integer num = new Double(Math.random()*15+1).intValue();
        List<String> codeInData = new ArrayList<>();
        first : for (int i = 0; i < num; i++) {
            Integer index = new Double(Math.random()*19).intValue();
            String code = list.get(index);
            String[] codes = code.split("_");
            String codeFront = codes[0] + "_" + codes[1] + "_" + codes[2];
            if (!(codeInData == null | codeInData.size() == 0)) {
                for (String s : codeInData) {
                    String[] ss = s.split("_");
                    String sFront = ss[0] + "_" + ss[1] + "_" + ss[2];
                    if (sFront.equals(codeFront)) {
                        i--;
                        continue first;
                    }
                }
            }
            codeInData.add(code);
        }
        JSONArray jsonArray = new JSONArray();
        for (int i = 0; i < num; i++) {
            jsonArray.add(groupData(listMap, codeInData.get(i)));
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        jsonObject.put("userId", forUserId());
        jsonObject.put("data", jsonArray);
        return jsonObject;
    }

    private JSONObject groupData(List<Map<String, String>> list, String dictionary_code_LRTZ) {
        if (list == null | list.size() == 0) {
            getLogger().error("standard_data查到的数据为空，请检查standard_data表");
            return null;
        }
        String dictionary_code_BZZB = relation.get(dictionary_code_LRTZ);
        JSONObject jsonObject = new JSONObject();
        for (Map<String, String> map : list) {
            if (map.get("dictionary_code").equals(dictionary_code_BZZB)) {
                Double smallValue = Double.valueOf(map.get("small_value"));
                Double bigValue = Double.valueOf(map.get("big_value"));
                Double difference = bigValue - smallValue;
                Double smaller = smallValue - difference;
                Double bigger = bigValue + difference;
                Integer indexValue = new Double(Math.random()*(bigger - smaller) + smaller).intValue();

                jsonObject.put("index_code", dictionary_code_LRTZ);
                jsonObject.put("index_name", map.get("dictionary_name"));
                jsonObject.put("index_value", String.valueOf(indexValue));
                jsonObject.put("isJudge", "1");
                jsonObject.put("source", "0");
                jsonObject.put("index_unit", map.get("indexUnit"));
            }
        }
        return jsonObject;
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
