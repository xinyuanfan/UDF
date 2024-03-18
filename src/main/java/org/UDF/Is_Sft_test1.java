package org.UDF;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.List;
@Description(name = "is_sft", value = "_FUNC_(bot_name, prompt, query, conditions) - Returns true if any condition in conditions matches")
public class Is_Sft_test1 extends GenericUDF{
    private PrimitiveObjectInspector botNameOI;
    private PrimitiveObjectInspector promptOI;
    private PrimitiveObjectInspector queryOI;
    private ListObjectInspector conditionsListOI;
    private ListObjectInspector innerListOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 4) {
            throw new UDFArgumentException("is_sft expects exactly 4 arguments.");
        }

        botNameOI = (PrimitiveObjectInspector) arguments[0];
        promptOI = (PrimitiveObjectInspector) arguments[1];
        queryOI = (PrimitiveObjectInspector) arguments[2];

        if (!(arguments[3] instanceof ListObjectInspector)) {
            throw new UDFArgumentException("The fourth argument must be a list of lists.");
        }
        conditionsListOI = (ListObjectInspector) arguments[3];
        innerListOI = (ListObjectInspector) conditionsListOI.getListElementObjectInspector();

        if (!(innerListOI.getListElementObjectInspector() instanceof PrimitiveObjectInspector)) {
            throw new UDFArgumentException("The elements of the conditions must be lists of strings.");
        }

        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String botName = botNameOI.getPrimitiveJavaObject(arguments[0].get()).toString();
        String prompt = promptOI.getPrimitiveJavaObject(arguments[1].get()).toString();
        String query = queryOI.getPrimitiveJavaObject(arguments[2].get()).toString();

        List<?> conditions = (List<?>) conditionsListOI.getList(arguments[3].get());

        for (Object conditionObj : conditions) {
            List<?> condition = (List<?>) innerListOI.getList(conditionObj);
            if (condition.size() < 3) continue; // Ensure there are at least 3 elements in the condition.

            String condBotName = condition.get(0).toString();
            String condPrompt = condition.get(1).toString();
            String condQuery = condition.get(2).toString();

            if (botName.contains(condBotName) && prompt.contains(condPrompt) && query.contains(condQuery)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("is_sft", children, ",");
    }
}
