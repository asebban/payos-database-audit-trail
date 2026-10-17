package ma.s2m.payos.audit.database;

import ma.s2m.payos.events.audit.AuditEvent;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the {@code payos_audit_event} row round-trip — {@link Writer} relies on {@link
 * AuditEventRow#toAuditEvent(Map)} reconstructing every field {@link AuditEventRow#toRow(AuditEvent)}
 * wrote, since that reconstructed event is exactly what gets appended to the configured {@code
 * IAuditTrailStore} (and therefore hashed into its hash-chain) on export.
 */
class AuditEventRowTest {

    @Test
    void toRowThenToAuditEventRoundTripsEveryStandardField() {
        AuditEvent original = AuditEvent.of("API_EXEC")
                .userId("john.doe").tenantId("acme").correlationId("corr-1").appId("app-1")
                .path("/api/payments").result("SUCCESS")
                .build();

        Map<String, Object> row = AuditEventRow.toRow(original);
        AuditEvent reconstructed = AuditEventRow.toAuditEvent(row);

        assertThat(reconstructed.getEventId()).isEqualTo(original.getEventId());
        assertThat(reconstructed.getEvent()).isEqualTo(original.getEvent());
        assertThat(reconstructed.getSchemaVersion()).isEqualTo(original.getSchemaVersion());
        assertThat(reconstructed.getUserId()).isEqualTo(original.getUserId());
        assertThat(reconstructed.getTenantId()).isEqualTo(original.getTenantId());
        assertThat(reconstructed.getCorrelationId()).isEqualTo(original.getCorrelationId());
        assertThat(reconstructed.getAppId()).isEqualTo(original.getAppId());
        assertThat(reconstructed.getPath()).isEqualTo(original.getPath());
        assertThat(reconstructed.getResult()).isEqualTo(original.getResult());
        // Millisecond precision only: createdAt is a java.util.Date column (type="timestamp" in
        // PayosAuditEvent.hbm.xml, the same convention HibernateNotificationStore already uses),
        // which truncates AuditEvent#getTimestamp()'s sub-millisecond Instant precision.
        assertThat(reconstructed.getTimestamp()).isEqualTo(original.getTimestamp().truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
    }

    @Test
    void toRowThenToAuditEventRoundTripsExtraFields() {
        AuditEvent original = AuditEvent.of("API_EXEC")
                .field("statusCode", 200)
                .build();

        Map<String, Object> row = AuditEventRow.toRow(original);
        AuditEvent reconstructed = AuditEventRow.toAuditEvent(row);

        assertThat(reconstructed.getExtra()).containsEntry("statusCode", 200);
    }

    @Test
    void toRowThenToAuditEventRoundTripsBusinessKeys() {
        AuditEvent original = AuditEvent.of("PAYMENT_REVERSAL")
                .businessKey("orderId", "ORD-1").businessKey("amount", 1500)
                .build();

        Map<String, Object> row = AuditEventRow.toRow(original);
        AuditEvent reconstructed = AuditEventRow.toAuditEvent(row);

        assertThat(reconstructed.getBusinessKeys()).containsEntry("orderId", "ORD-1");
        assertThat(reconstructed.getBusinessKeys()).containsEntry("amount", 1500);
    }

    @Test
    void toRowStoresTenantIdAsNullRatherThanAuditEventsOwnNotSetPlaceholder() {
        AuditEvent withoutTenant = AuditEvent.of("SYSTEM_STARTUP").build();

        Map<String, Object> row = AuditEventRow.toRow(withoutTenant);

        assertThat(withoutTenant.getTenantId()).isEqualTo("-");
        assertThat(row.get("tenantId")).isNull();
    }

    @Test
    void toRowStoresCreatedAtAsADateDerivedFromTheEventsTimestamp() {
        AuditEvent event = AuditEvent.of("SYSTEM_STARTUP").build();

        Map<String, Object> row = AuditEventRow.toRow(event);

        assertThat(row.get("createdAt")).isInstanceOf(Date.class);
        // Date.from(...) truncates to millisecond precision — see the comment on the round-trip test above.
        assertThat(((Date) row.get("createdAt")).toInstant())
                .isEqualTo(event.getTimestamp().truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
    }

    @Test
    void toAuditEventOnARowWithNoBusinessKeysOrFieldsJsonReconstructsAnEmptyEventCleanly() {
        AuditEvent event = AuditEvent.of("SYSTEM_SHUTDOWN").build();
        Map<String, Object> row = AuditEventRow.toRow(event);

        AuditEvent reconstructed = AuditEventRow.toAuditEvent(row);

        assertThat(reconstructed.getBusinessKeys()).isEmpty();
        assertThat(reconstructed.getExtra()).isEmpty();
    }

    @Test
    void toRowSerializesAListValuedExtraFieldAsJsonAndToAuditEventReconstructsIt() {
        AuditEvent event = AuditEvent.of("AUTHZ_DENIED")
                .field("requiredRoles", List.of("ADMIN", "OPERATOR"))
                .build();

        Map<String, Object> row = AuditEventRow.toRow(event);
        AuditEvent reconstructed = AuditEventRow.toAuditEvent(row);

        assertThat(reconstructed.getExtra().get("requiredRoles")).isEqualTo(List.of("ADMIN", "OPERATOR"));
    }
}
