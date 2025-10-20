package io.datalatte.etl.model;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class GithubEvent {
    String id;
    String eventType;
    String actor;
    String repo;
    String org;       // nullable
    String createdAt;
    boolean isPublic;
}
