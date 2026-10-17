package ma.s2m.payos.audit.database;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import ma.s2m.payos.events.audit.AuditEvent;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps an {@link AuditEvent} to/from the {@code payos_audit_event} row shape declared in {@code
 * mappings/PayosAuditEvent.hbm.xml}. {@code businessKeysJson}/{@code fieldsJson} hold {@link
 * AuditEvent#getBusinessKeys()}/{@link AuditEvent#getExtra()} as Jackson-serialized JSON text,
 * since a dynamic-map Hibernate mapping has no native JSON column type.
 *
 * <p>{@code tenantId}/{@code userId}/{@code appId}/{@code correlationId}/{@code path}/{@code
 * result} are stored as {@code null} rather than {@link AuditEvent}'s own {@code "-"}
 * not-set placeholder — {@link AuditEvent}'s constructor re-applies that placeholder on the way
 * back in, so the round-trip is exact either way, but {@code null} is the more natural "unset"
 * representation for a database column.
 */
final class AuditEventRow {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AuditEventRow() {
    }

    static Map<String, Object> toRow(AuditEvent event) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("eventId", event.getEventId());
        row.put("schemaVersion", event.getSchemaVersion());
        row.put("eventType", event.getEvent());
        row.put("tenantId", blankToNull(event.getTenantId()));
        row.put("correlationId", blankToNull(event.getCorrelationId()));
        row.put("appId", blankToNull(event.getAppId()));
        row.put("path", blankToNull(event.getPath()));
        row.put("userId", blankToNull(event.getUserId()));
        row.put("result", blankToNull(event.getResult()));
        row.put("operation", event.getOperation());
        row.put("resourceType", event.getResourceType());
        row.put("resourceId", event.getResourceId());
        row.put("businessKeysJson", toJsonOrNull(event.getBusinessKeys()));
        row.put("fieldsJson", toJsonOrNull(event.getExtra()));
        row.put("createdAt", Date.from(event.getTimestamp()));
        row.put("exportedAt", null);
        return row;
    }

    /** Reconstructs an {@link AuditEvent} from an already-persisted row — used by {@link Writer} on export. */
    static AuditEvent toAuditEvent(Map<String, Object> row) {
        AuditEvent.Builder builder = AuditEvent.of((String) row.get("eventType"))
                .eventId((String) row.get("eventId"))
                .userId((String) row.get("userId"))
                .tenantId((String) row.get("tenantId"))
                .appId((String) row.get("appId"))
                .correlationId((String) row.get("correlationId"))
                .path((String) row.get("path"))
                .result((String) row.get("result"))
                .operation((String) row.get("operation"))
                .resourceType((String) row.get("resourceType"))
                .resourceId((String) row.get("resourceId"));

        Object schemaVersion = row.get("schemaVersion");
        if (schemaVersion instanceof Number n) {
            builder.schemaVersion(n.intValue());
        }
        Object createdAt = row.get("createdAt");
        if (createdAt instanceof Date date) {
            builder.timestamp(date.toInstant());
        }

        Map<String, Object> businessKeys = fromJsonOrEmpty((String) row.get("businessKeysJson"));
        if (!businessKeys.isEmpty()) {
            builder.businessKeys(businessKeys);
        }
        fromJsonOrEmpty((String) row.get("fieldsJson")).forEach(builder::field);

        return builder.build();
    }

    private static String toJsonOrNull(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(map);
        } catch (Exception e) {
            return null;
        }
    }

    private static Map<String, Object> fromJsonOrEmpty(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() { });
        } catch (Exception e) {
            return Map.of();
        }
    }

    private static String blankToNull(String value) {
        return (value == null || value.isBlank() || "-".equals(value)) ? null : value;
    }
}
