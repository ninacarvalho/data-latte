package io.datalatte.etl.transformer;

import java.util.LinkedHashMap;
import java.util.Map;

public class FakeUserTransformer implements Transformer<Map<String,Object>, Map<String,Object>> {

    @Override
    public Map<String, Object> apply(Map<String, Object> in) {
        if (in == null) throw new IllegalArgumentException("input is null");

        Object idRaw = in.get("id");
        Object first = in.get("first_name");
        Object last = in.get("last_name");
        Object email = in.get("email");
        Object active = in.get("active");

        if (!(idRaw instanceof Number n)) throw new IllegalArgumentException("id missing or not numeric");
        if (email == null) throw new IllegalArgumentException("email required");

        String fullName = ((first == null ? "" : first.toString()).trim()
                + " " +
                (last == null ? "" : last.toString()).trim()).trim();

        String status = truthy(active) ? "ACTIVE" : "INACTIVE";

        Map<String,Object> out = new LinkedHashMap<>();
        out.put("id", n.intValue());
        out.put("fullName", fullName);
        out.put("email", email.toString());
        out.put("status", status);
        return out;
    }

    private boolean truthy(Object v) {
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.intValue() != 0;
        if (v instanceof String s) return s.equalsIgnoreCase("true") || s.equals("1") || s.equalsIgnoreCase("yes");
        return false;
    }
}
