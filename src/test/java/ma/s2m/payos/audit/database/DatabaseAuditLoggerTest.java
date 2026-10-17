package ma.s2m.payos.audit.database;

import ma.s2m.DynamicDataAccessService;
import ma.s2m.payos.config.IConfigSpec;
import ma.s2m.payos.config.PayOSConfig;
import ma.s2m.payos.exceptions.FatalException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers {@link DatabaseAuditLogger}'s system-event path: synchronous, per-call writes to
 * {@code payos_audit_event} (tenant-scoped or platform-scoped), per-event-type suppression, the
 * fatal/non-fatal split between a startup schema failure ({@link FatalException}, per §5.3 of
 * {@code atomic-db-audit-queue-v17-2026-10-15.md}) and a per-event write failure (logged and
 * swallowed — a system event must never crash its caller).
 */
class DatabaseAuditLoggerTest {

    private static final String MAPPING_FILE = "mappings/PayosAuditEvent.hbm.xml";

    private String tenantId;
    private DynamicDataAccessService multiTenantService;
    private DynamicDataAccessService platformService;
    private DatabaseAuditLogger auditLogger;

    @BeforeEach
    void setUp() {
        tenantId = "tenant-" + UUID.randomUUID().toString().replace("-", "");
        String tenantDbUrl = "jdbc:h2:mem:audit_tenant_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
        String platformDbUrl = "jdbc:h2:mem:audit_platform_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";

        Map<String, DynamicDataAccessService.TenantConfig> registry = new LinkedHashMap<>();
        // ddlAuto "none": the tenant's own SessionFactory never creates anything — proves the table
        // comes from DatabaseAuditLogger's own ensureSchemaFor call, not from this.
        registry.put(tenantId, new DynamicDataAccessService.TenantConfig(
                tenantDbUrl, "sa", "", "org.h2.Driver", "org.hibernate.dialect.H2Dialect",
                "none", "PUBLIC", MAPPING_FILE));
        multiTenantService = new DynamicDataAccessService(registry);

        DynamicDataAccessService.TenantConfig platformConfig = new DynamicDataAccessService.TenantConfig(
                platformDbUrl, "sa", "", "org.h2.Driver", "org.hibernate.dialect.H2Dialect",
                "none", "PUBLIC", MAPPING_FILE);
        platformService = new DynamicDataAccessService(platformConfig);

        PayOSConfig.setDatabaseService(multiTenantService);
        PayOSConfig.setPlatformDatabaseService(platformService);
        PayOSConfig.settings.put(IConfigSpec.Multitenancy.name,
                Map.of(IConfigSpec.Multitenancy.Tenants.name, Map.of(tenantId, Map.of())));

        auditLogger = new DatabaseAuditLogger();
        // Large poll interval: the background Writer's first cycle still fires once immediately,
        // but never again during a test's short lifetime — keeping assertions deterministic.
        auditLogger.initialize(Map.of("poll-interval-seconds", 3600));
    }

    @AfterEach
    void tearDown() {
        auditLogger.shutdown();
        PayOSConfig.setDatabaseService(null);
        PayOSConfig.setPlatformDatabaseService(null);
        PayOSConfig.settings.remove(IConfigSpec.Multitenancy.name);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> fetchTenantRows(String eventType) {
        multiTenantService.setCurrentTenant(tenantId);
        try {
            Map<String, Object> params = multiTenantService.newParams();
            params.put("t", eventType);
            return (List<Map<String, Object>>) multiTenantService.list(
                    "from PayosAuditEvent e where e.eventType = :t", params, 50);
        } finally {
            multiTenantService.clearCurrentTenant();
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> fetchPlatformRows(String eventType) {
        Map<String, Object> params = platformService.newParams();
        params.put("t", eventType);
        return (List<Map<String, Object>>) platformService.list(
                "from PayosAuditEvent e where e.eventType = :t", params, 50);
    }

    @Test
    void logApiExecutionWritesARowForTheGivenTenantIndependentlyOfItsOwnDdlAuto() {
        auditLogger.logApiExecution("john.doe", tenantId, "corr-1", "/api/payments", 200);

        List<Map<String, Object>> rows = fetchTenantRows("API_EXEC");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("userId")).isEqualTo("john.doe");
        assertThat(rows.get(0).get("result")).isEqualTo("SUCCESS");
        assertThat(rows.get(0).get("path")).isEqualTo("/api/payments");
        assertThat(rows.get(0).get("exportedAt")).isNull();
    }

    @Test
    void logApiExecutionWithAnErrorStatusCodeIsRecordedWithResultError() {
        auditLogger.logApiExecution("john.doe", tenantId, "corr-1b", "/api/payments", 500);

        List<Map<String, Object>> rows = fetchTenantRows("API_EXEC");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("result")).isEqualTo("ERROR");
    }

    @Test
    void logStartupWithNoTenantWritesToThePlatformDatabaseNotAnyTenantDatabase() {
        auditLogger.logStartup("2.6.0");

        assertThat(fetchPlatformRows("SYSTEM_STARTUP")).hasSize(1);
        assertThat(fetchTenantRows("SYSTEM_STARTUP")).isEmpty();
    }

    @Test
    void disabledEventTypesAreNeverWrittenToTheDatabase() {
        DatabaseAuditLogger suppressingLogger = new DatabaseAuditLogger();
        suppressingLogger.initialize(Map.of(
                "disabled-event-types", List.of("API_EXEC"),
                "poll-interval-seconds", 3600));
        try {
            suppressingLogger.logApiExecution("john.doe", tenantId, "corr-2", "/api/x", 200);

            assertThat(fetchTenantRows("API_EXEC")).isEmpty();
        } finally {
            suppressingLogger.shutdown();
        }
    }

    @Test
    void anEventTypeNotInTheDisabledListIsStillWritten() {
        DatabaseAuditLogger suppressingLogger = new DatabaseAuditLogger();
        suppressingLogger.initialize(Map.of(
                "disabled-event-types", List.of("API_EXEC"),
                "poll-interval-seconds", 3600));
        try {
            suppressingLogger.logStartup("2.6.0");

            assertThat(fetchPlatformRows("SYSTEM_STARTUP")).hasSize(1);
        } finally {
            suppressingLogger.shutdown();
        }
    }

    @Test
    void initializeThrowsFatalExceptionWhenATenantsTableCannotBeEnsured() {
        String badTenantId = "bad-tenant";
        Map<String, DynamicDataAccessService.TenantConfig> badRegistry = new LinkedHashMap<>();
        badRegistry.put(badTenantId, new DynamicDataAccessService.TenantConfig(
                "jdbc:h2:mem:does-not-matter", "sa", "", "org.h2.NoSuchDriver",
                "org.hibernate.dialect.H2Dialect", "none", "PUBLIC", MAPPING_FILE));
        DynamicDataAccessService badMultiTenant = new DynamicDataAccessService(badRegistry);

        PayOSConfig.setDatabaseService(badMultiTenant);
        PayOSConfig.settings.put(IConfigSpec.Multitenancy.name,
                Map.of(IConfigSpec.Multitenancy.Tenants.name, Map.of(badTenantId, Map.of())));

        DatabaseAuditLogger failingLogger = new DatabaseAuditLogger();

        assertThatThrownBy(() -> failingLogger.initialize(Map.of()))
                .isInstanceOf(FatalException.class);
    }

    @Test
    void aWriteFailureForAnUnregisteredTenantIsLoggedAndSwallowedNeverThrown() {
        assertThatCode(() -> auditLogger.logApiExecution(
                "john.doe", "unregistered-tenant-xyz", "corr-3", "/api/y", 200))
                .doesNotThrowAnyException();
    }

    @Test
    void sessionIdIsMaskedInLoggedSessionEvents() {
        auditLogger.logSessionCreated("a-very-long-session-identifier-value", tenantId, "corr-4");

        List<Map<String, Object>> rows = fetchTenantRows("SESSION_CREATED");
        assertThat(rows).hasSize(1);
        assertThat((String) rows.get(0).get("fieldsJson")).contains("a-very-l").doesNotContain("identifier-value");
    }
}
