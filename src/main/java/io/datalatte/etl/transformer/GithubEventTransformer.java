package io.datalatte.etl.transformer;

import io.datalatte.etl.model.GithubEvent;
import java.util.Map;

public class GithubEventTransformer implements Transformer<Map<String,Object>, GithubEvent> {

    @Override
    public GithubEvent apply(Map<String,Object> in) {
        if (in == null) throw new IllegalArgumentException("input is null");

        String id = str(in.get("id"));
        String type = str(in.get("type"));
        if (id.isBlank() || type.isBlank()) {
            throw new IllegalArgumentException("id and type required");
        }

        Map<String,Object> actor = getMap(in.get("actor"));
        Map<String,Object> repo  = getMap(in.get("repo"));
        Map<String,Object> org   = getMap(in.get("org"));

        return GithubEvent.builder()
                .id(id)
                .eventType(type)
                .actor(str(actor.get("login")))
                .repo(str(repo.get("name")))
                .org(org.isEmpty() ? null : str(org.get("login")))
                .createdAt(str(in.get("created_at")))
                .isPublic(bool(in.get("public")))
                .build();
    }

    @SuppressWarnings("unchecked")
    private static Map<String,Object> getMap(Object o) {
        return (o instanceof Map<?,?> m) ? (Map<String,Object>) m : Map.of();
    }

    private static String str(Object o) {
        return o == null ? "" : o.toString().trim();
    }

    private static boolean bool(Object o) {
        if (o instanceof Boolean b) return b;
        if (o instanceof String s) return s.equalsIgnoreCase("true") || s.equals("1");
        if (o instanceof Number n) return n.intValue() != 0;
        return false;
    }
}
