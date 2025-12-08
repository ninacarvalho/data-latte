package io.datalatte.etl.model;

import lombok.Builder;
import lombok.Value;

/**
 * Represents a simplified GitHub event, tailored for this ETL process.
 * This is an immutable data class.
 *
 * {@code @Value} is a Lombok annotation that bundles the features of
 * {@code @ToString}, {@code @EqualsAndHashCode}, {@code @Getter} on all fields,
 * and {@code @AllArgsConstructor}, and makes the class final and its fields private and final by default.
 *
 * {@code @Builder} is a Lombok annotation that produces complex builder APIs for your classes.
 */
@Value
@Builder
public class GithubEvent {
    /** The unique identifier for the event. */
    String id;

    /** The type of the event (e.g., "PushEvent", "WatchEvent"). */
    String eventType;

    /** The login name of the user who triggered the event. */
    String actor;

    /** The name of the repository where the event occurred (e.g., "owner/repo"). */
    String repo;

    /** The login name of the organization, if the event occurred within one. This field is nullable. */
    String org;

    /** The timestamp of when the event was created, in ISO 8601 format. */
    String createdAt;

    /** A boolean indicating whether the event is public. */
    boolean isPublic;
}
