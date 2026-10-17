package ma.s2m.payos.audit.database;

import ma.s2m.DynamicDataAccessService;
import ma.s2m.payos.config.IConfigSpec;
import ma.s2m.payos.config.PayOSConfig;
import ma.s2m.payos.events.audit.AuditEvent;
import ma.s2m.payos.events.audit.store.AuditTrailQuery;
import ma.s2m.payos.events.audit.store.AuditTrailQueryResult;
import ma.s2m.payos.events.audit.store.IAuditTrailStore;
import ma.s2m.payos.events.audit.store.VerificationResult;
import ma.s2m.payos.events.audit.store.VerificationScope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link Writer}'s export-to-store and purge-past-retention cycle — the {@code database}
 * category's counterpart to {@code BufferedAuditLogger}'s own {@code Writer}, except reading
 * already-committed {@code payos_audit_event} rows instead of draining an in-memory buffer.
 */
class WriterTest {

    private static final String MAPPING_FILE = "mappings/PayosAuditEvent.hbm.xml";

    private String tenantId;
    private DynamicDataAccessService multiTenantService;
    private FakeAuditTrailStore store;

    @BeforeEach
    void setUp() {
        tenantId = "tenant-" + UUID.randomUUID().toString().replace("-", "");
        String tenantDbUrl = "jdbc:h2:mem:writer_tenant_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";

        Map<String, DynamicDataAccessService.TenantConfig> registry = new LinkedHashMap<>();
        registry.put(tenantId, new DynamicDataAccessService.TenantConfig(
                tenantDbUrl, "sa", "", "org.h2.Driver", "org.hibernate.dialect.H2Dialect",
                "update", "PUBLIC", MAPPING_FILE));
        multiTenantService = new DynamicDataAccessService(registry);

        PayOSConfig.setDatabaseService(multiTenantService);
        PayOSConfig.setPlatformDatabaseService(null);
        PayOSConfig.settings.put(IConfigSpec.Multitenancy.name,
                Map.of(IConfigSpec.Multitenancy.Tenants.name, Map.of(tenantId, Map.of())));

        store = new FakeAuditTrailStore();
    }

    @AfterEach
    void tearDown() {
        PayOSConfig.setDatabaseService(null);
        PayOSConfig.settings.remove(IConfigSpec.Multitenancy.name);
    }

    private void insertRow(String eventId, String eventType, Instant createdAt, boolean exported) {
        multiTenantService.setCurrentTenant(tenantId);
        try {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("eventId", eventId);
            row.put("schemaVersion", 1);
            row.put("eventType", eventType);
            row.put("createdAt", Date.from(createdAt));
            row.put("exportedAt", exported ? Date.from(createdAt) : null);
            multiTenantService.save("PayosAuditEvent", row);
        } finally {
            multiTenantService.clearCurrentTenant();
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> fetchAllRows() {
        multiTenantService.setCurrentTenant(tenantId);
        try {
            return (List<Map<String, Object>>) multiTenantService.list("from PayosAuditEvent", Map.of(), 100);
        } finally {
            multiTenantService.clearCurrentTenant();
        }
    }

    @Test
    void exportsEveryUnexportedRowToTheBoundStoreAndMarksItExported() {
        insertRow("evt-1", "API_EXEC", Instant.now(), false);
        Writer writer = new Writer(store, 90, 3600);

        writer.runOneCycle();

        assertThat(store.appended).hasSize(1);
        assertThat(store.appended.get(0).getEventId()).isEqualTo("evt-1");
        List<Map<String, Object>> rows = fetchAllRows();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("exportedAt")).isNotNull();
    }

    @Test
    void exportsRowsInChronologicalOrderOldestFirst() {
        Instant now = Instant.now();
        insertRow("evt-newer", "API_EXEC", now, false);
        insertRow("evt-older", "API_EXEC", now.minusSeconds(60), false);
        Writer writer = new Writer(store, 90, 3600);

        writer.runOneCycle();

        assertThat(store.appended).extracting(AuditEvent::getEventId)
                .containsExactly("evt-older", "evt-newer");
    }

    @Test
    void alreadyExportedRowsAreNotReExported() {
        insertRow("evt-already", "API_EXEC", Instant.now(), true);
        Writer writer = new Writer(store, 90, 3600);

        writer.runOneCycle();

        assertThat(store.appended).isEmpty();
    }

    @Test
    void purgesExportedRowsOlderThanRetentionDays() {
        insertRow("evt-old-exported", "API_EXEC", Instant.now().minus(100, ChronoUnit.DAYS), true);
        Writer writer = new Writer(store, 90, 3600);

        writer.runOneCycle();

        assertThat(fetchAllRows()).isEmpty();
    }

    @Test
    void doesNotPurgeExportedRowsWithinTheRetentionWindow() {
        insertRow("evt-recent-exported", "API_EXEC", Instant.now().minus(1, ChronoUnit.DAYS), true);
        Writer writer = new Writer(store, 90, 3600);

        writer.runOneCycle();

        assertThat(fetchAllRows()).hasSize(1);
    }

    @Test
    void neverPurgesARowThatHasNotBeenExportedYetEvenIfOlderThanRetention() {
        insertRow("evt-old-not-exported", "API_EXEC", Instant.now().minus(200, ChronoUnit.DAYS), false);
        Writer writer = new Writer(null, 90, 3600);

        writer.runOneCycle();

        assertThat(fetchAllRows()).hasSize(1);
    }

    @Test
    void aFailingStoreLeavesTheRowUnexportedForRetryOnTheNextCycle() {
        insertRow("evt-fails", "API_EXEC", Instant.now(), false);
        FakeAuditTrailStore failingStore = new FakeAuditTrailStore();
        failingStore.failNext = true;
        Writer writer = new Writer(failingStore, 90, 3600);

        writer.runOneCycle();

        List<Map<String, Object>> rows = fetchAllRows();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("exportedAt")).isNull();
    }

    private static final class FakeAuditTrailStore implements IAuditTrailStore {
        private final List<AuditEvent> appended = new ArrayList<>();
        private boolean failNext = false;

        @Override
        public String type() {
            return "fake";
        }

        @Override
        public void append(AuditEvent event) {
            if (failNext) {
                failNext = false;
                throw new RuntimeException("simulated store failure");
            }
            appended.add(event);
        }

        @Override
        public VerificationResult verify(VerificationScope scope) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AuditTrailQueryResult find(AuditTrailQuery query) {
            throw new UnsupportedOperationException();
        }
    }
}
