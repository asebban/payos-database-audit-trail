package ma.s2m.payos.audit.database;

import ma.s2m.payos.config.PayOSConfig;
import ma.s2m.payos.database.AtomicWriteContext;
import ma.s2m.payos.database.DatabaseServiceInitializer;
import ma.s2m.payos.database.IDatabaseService;
import ma.s2m.payos.events.audit.AuditEvent;
import ma.s2m.payos.events.audit.IAuditLogger;
import ma.s2m.payos.events.audit.store.IAuditTrailStore;
import ma.s2m.payos.events.metrics.MetricSample;
import ma.s2m.payos.events.metrics.MetricsRecorder;
import ma.s2m.payos.exceptions.FatalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@code database} category {@link IAuditLogger}: every event — system (the typed lifecycle
 * methods below) and script-originated ({@code $Audit}, via {@code AuditBinding} in the kernel) —
 * lands in the same {@code payos_audit_event} table, through the single {@link
 * #logEvent(AuditEvent)} override below, mapped by the shared {@link AuditEventRow} helper. The
 * two call sites differ only in which session ends up carrying the write: {@link #logEvent}
 * checks {@link AtomicWriteContext#get()} first — when a caller (today, only {@code AuditBinding})
 * has set one, the write goes straight through that session, joining its transaction; otherwise
 * this falls back to its own short-lived, auto-committed session against the shared multi-tenant
 * service (tenant-scoped) or straight to the platform database (platform-scoped), since no
 * request-owned {@code $DB}/{@code $PlatformDB} session can be assumed to exist at the point a
 * system event fires. {@code AuditBinding} never checks whether this class (or whichever {@code
 * IAuditLogger} happens to be active) actually reads {@link AtomicWriteContext} — it always sets
 * it and always calls {@code logEvent(...)}; an implementation that never looks (e.g. {@code
 * payos-buffered-audit-trail}'s {@code BufferedAuditLogger}) simply logs the event the normal way,
 * without ever becoming atomic. This class is the one that opted in, by checking the context here.
 *
 * <p>Deliberately <strong>not</strong> an in-memory buffer like {@code payos-buffered-audit-trail}'s
 * {@code BufferedAuditLogger} — each {@link #logEvent(AuditEvent)} call is a direct, synchronous
 * database write (its own small transaction, auto-committed, unless {@link AtomicWriteContext} is
 * set), so an event survives a crash the instant it's accepted rather than only once a background
 * thread later drains it. The background {@link Writer} here has a different job: export
 * already-committed rows to the configured {@link IAuditTrailStore} and purge rows past the
 * configured retention — see that class's own Javadoc.
 *
 * <p>See {@code payos/docs/architects/atomic-db-audit-queue-v17-2026-10-15.md} §3.5/§3.6/§5 for the
 * full design this class implements.
 */
public final class DatabaseAuditLogger implements IAuditLogger {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseAuditLogger.class);

    /** Matches {@code audit-trail.implementation.category: "database"}. */
    public static final String TYPE = "database";

    /** Entity-name used by {@code mappings/PayosAuditEvent.hbm.xml} — never a generated Java class. */
    static final String ENTITY_NAME = "PayosAuditEvent";
    private static final String MAPPING_FILE = "mappings/PayosAuditEvent.hbm.xml";

    private static final String PARAM_DISABLED_EVENT_TYPES = "disabled-event-types";
    private static final String PARAM_RETENTION_DAYS = "retention-days";
    private static final String PARAM_POLL_INTERVAL_SECONDS = "poll-interval-seconds";

    static final int DEFAULT_RETENTION_DAYS = 90;
    static final int DEFAULT_POLL_INTERVAL_SECONDS = 10;

    private volatile Set<String> disabledEventTypes = Set.of();
    private volatile IAuditTrailStore store;
    @SuppressWarnings("unused")
    private volatile Writer writer;
    private volatile Thread writerThread;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public void bindStore(IAuditTrailStore store) {
        this.store = store;
    }

    /**
     * Ensures {@code payos_audit_event} exists for every known tenant and the platform database
     * (§5.2/§5.3) — thrown as {@link FatalException} on any failure, which the kernel's own
     * existing {@code catch (Exception e) { ...; System.exit(1); }} around {@code
     * AuditTrailInitializer.initialize(...)} already turns into a clean startup abort, with zero
     * kernel code changes required. Then starts the background {@link Writer}.
     */
    @Override
    public void initialize(Map<String, Object> parameters) {
        this.disabledEventTypes = parseDisabledEventTypes(parameters);
        int retentionDays = intOrDefault(parameters == null ? null : parameters.get(PARAM_RETENTION_DAYS), DEFAULT_RETENTION_DAYS);
        int pollIntervalSeconds = intOrDefault(parameters == null ? null : parameters.get(PARAM_POLL_INTERVAL_SECONDS), DEFAULT_POLL_INTERVAL_SECONDS);

        ensureSchemaForEveryKnownDatabase();

        Writer newWriter = new Writer(store, retentionDays, pollIntervalSeconds);
        Thread t = new Thread(newWriter, "audit-writer-database");
        t.setDaemon(true);
        t.start();
        this.writer = newWriter;
        this.writerThread = t;
        logger.info("DatabaseAuditLogger initialized: retention-days={}, poll-interval-seconds={}, disabled-event-types={}",
                retentionDays, pollIntervalSeconds, disabledEventTypes);
    }

    @Override
    public void shutdown() {
        Thread t = writerThread;
        if (t != null) {
            t.interrupt();
        }
    }

    /**
     * {@code ensureSchemaFor(...)} only creates the physical table, via a throwaway {@code
     * SessionFactory} scoped to {@link #MAPPING_FILE} alone — it does <strong>not</strong> make
     * {@code "PayosAuditEvent"} a resolvable entity-name on the tenant's own, ongoing {@code $DB}
     * {@code SessionFactory} (the one {@code AuditBinding} actually writes through). A tenant's own
     * {@code $DB} mapping files come exclusively from the {@code mapping-files} array of every
     * {@code applications[]} entry that tenant is authorized for (unioned, with {@code extends}
     * inheritance) — never from a {@code database-service.mapping-files} key, which the kernel
     * never reads for this purpose. The deployment must list {@link #MAPPING_FILE} in at least one
     * such authorized application (and, for the platform database, in {@code
     * platform-database-service.mapping-files}, which <em>is</em> a real, directly-read key) — see
     * [configuration/audit-trail.md §Required: register the mapping file]. A deployment that forgot
     * this step would otherwise only discover it the first time a script actually calls {@code
     * $Audit.logEvent(...)}, as a raw {@code org.hibernate.MappingException: Unknown entity};
     * probing it here at startup turns that into an immediate, actionable failure instead.
     */
    private void ensureSchemaForEveryKnownDatabase() {
        IDatabaseService multiTenant = PayOSConfig.getDatabaseService();
        if (multiTenant != null) {
            for (String tenantId : knownTenantIds()) {
                try {
                    IDatabaseService.DedicatedTenantSession session = multiTenant.openDedicatedSessionFor(tenantId);
                    try {
                        session.ensureSchemaFor(MAPPING_FILE);
                        // get(...) resolves the current tenant the same way save()/list() do, even
                        // on a dedicated session — without this, the probe itself fails with
                        // "Unable to resolve current tenant" rather than testing what it's meant to.
                        session.setCurrentTenant(tenantId);
                        try {
                            verifyEntityIsResolvable(session, "tenant '" + tenantId + "'",
                                    "the 'mapping-files' array of at least one applications[] entry authorized "
                                            + "for tenant '" + tenantId + "' (a tenant's own $DB mapping files "
                                            + "come from its authorized applications, never from a "
                                            + "database-service.mapping-files key)");
                        } finally {
                            session.clearCurrentTenant();
                        }
                    } finally {
                        session.close();
                    }
                } catch (RuntimeException e) {
                    throw new FatalException(
                            "payos-database-audit-trail: could not ensure 'payos_audit_event' exists for tenant '"
                                    + tenantId + "' — refusing to start", e);
                }
            }
        }
        IDatabaseService platform = PayOSConfig.getPlatformDatabaseService();
        if (platform != null) {
            try {
                platform.ensureSchemaFor(MAPPING_FILE);
                verifyEntityIsResolvable(platform, "the platform database", "platform-database-service.mapping-files");
            } catch (RuntimeException e) {
                throw new FatalException(
                        "payos-database-audit-trail: could not ensure 'payos_audit_event' exists on the "
                                + "platform database — refusing to start", e);
            }
        }
    }

    /**
     * Probes whether {@code "PayosAuditEvent"} is actually resolvable on {@code session}'s own
     * {@code SessionFactory} — the same one {@code $DB}/{@code $PlatformDB} use, since {@code
     * openDedicatedSessionFor(...)} shares the tenant's cached {@code SessionFactory}. A harmless
     * {@code get(...)} for an id that cannot exist either returns {@code null} (resolvable) or
     * throws (not resolvable) — never mutates anything.
     */
    private static void verifyEntityIsResolvable(IDatabaseService session, String scopeLabel, String configPath) {
        try {
            session.get(ENTITY_NAME, "__payos_startup_probe__");
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "payos-database-audit-trail: 'payos_audit_event' table exists for " + scopeLabel
                            + ", but \"" + ENTITY_NAME + "\" is not a resolvable entity on its own $DB/$PlatformDB "
                            + "session — add \"" + MAPPING_FILE + "\" (from the payos-database-audit-trail jar) to "
                            + configPath + ". Cause: " + e, e);
        }
    }

    /**
     * Tenants this probe actually needs to cover: every {@code multitenancy.tenants} entry
     * authorized for at least one declared {@code applications[]} entry — a tenant with zero
     * authorized applications can never have a script run under it, so it can never reach {@code
     * $Audit} either. See {@link DatabaseServiceInitializer#tenantsAuthorizedForAnyApplication}.
     */
    static Set<String> knownTenantIds() {
        return DatabaseServiceInitializer.tenantsAuthorizedForAnyApplication(PayOSConfig.settings);
    }

    // -----------------------------------------------------------------------
    // Core method — every typed method below delegates here
    // -----------------------------------------------------------------------

    @Override
    public void logEvent(AuditEvent event) {
        if (event == null) {
            return;
        }

        // $Audit path (AuditBinding, payos kernel): when set, this write MUST go through the
        // caller's own request-owned transaction, never one this class opens itself. No
        // disabled-event-types filtering here (that list is scoped to the kernel's own frequent
        // system events only — a script's own $Audit.logEvent(...) is never silently filtered)
        // and no try/catch swallowing either — a failure must propagate straight back to the
        // script so its transaction rolls back, unlike the system-event path below.
        IDatabaseService atomicitySession = AtomicWriteContext.get();
        if (atomicitySession != null) {
            atomicitySession.save(ENTITY_NAME, AuditEventRow.toRow(event));
            return;
        }

        if (disabledEventTypes.contains(event.getEvent())) {
            MetricsRecorder.record(new MetricSample("audit.event.suppressed", null, MetricSample.Type.COUNTER,
                    1, "events", event.getTenantId(), event.getCorrelationId(),
                    Map.of("eventType", event.getEvent())));
            return;
        }

        String tenantId = blankToNull(event.getTenantId());
        try {
            if (tenantId != null) {
                writeToTenant(tenantId, AuditEventRow.toRow(event));
            } else {
                writeToPlatform(AuditEventRow.toRow(event));
            }
        } catch (RuntimeException e) {
            // A system event's write failure must never crash whatever kernel code path triggered
            // it (auth, API execution, ...) — unlike $Audit's own strict path (AuditBinding), which
            // is deliberately allowed to fail the script's transaction outright. Logged loudly so
            // the loss is visible, never silently swallowed.
            logger.error("payos-database-audit-trail: failed to write audit event eventId={} eventType={} tenantId={}: {}",
                    event.getEventId(), event.getEvent(), tenantId, e.toString(), e);
        }
    }

    /**
     * Same lightweight pattern as {@code HibernateNotificationStore.withTenant(...)}: set/clear the
     * ambient tenant on the shared, pool-backed multi-tenant service and let {@code save(...)} open
     * and auto-commit its own short-lived, per-call session — never {@code
     * openDedicatedSessionFor(...)}, which builds a whole separate {@code DynamicDataAccessService}
     * wrapper per call and is reserved for a caller needing an exclusive, manually-closed handle
     * (schema creation at startup, {@code payos-job-scheduler}'s one-per-job-execution use).
     */
    private void writeToTenant(String tenantId, Map<String, Object> row) {
        IDatabaseService multiTenant = PayOSConfig.getDatabaseService();
        if (multiTenant == null) {
            throw new IllegalStateException("No multi-tenant database service configured — cannot write audit event for tenant '" + tenantId + "'");
        }
        multiTenant.setCurrentTenant(tenantId);
        try {
            multiTenant.save(ENTITY_NAME, row);
        } finally {
            multiTenant.clearCurrentTenant();
        }
    }

    private void writeToPlatform(Map<String, Object> row) {
        IDatabaseService platform = PayOSConfig.getPlatformDatabaseService();
        if (platform == null) {
            throw new IllegalStateException("No platform database service configured — cannot write platform-level audit event");
        }
        platform.save(ENTITY_NAME, row);
    }

    // -----------------------------------------------------------------------
    // Typed convenience methods — build AuditEvent and delegate to logEvent
    // (mirrors payos-buffered-audit-trail's BufferedAuditLogger field mapping exactly)
    // -----------------------------------------------------------------------

    @Override
    public void logAuthSuccess(String userId, String tenantId, String correlationId, String appId) {
        logEvent(AuditEvent.of("AUTH_SUCCESS")
                .userId(userId).tenantId(tenantId).correlationId(correlationId).appId(appId)
                .result("SUCCESS").build());
    }

    @Override
    public void logAuthFailure(String reason, String tenantId, String correlationId, String path) {
        logEvent(AuditEvent.of("AUTH_FAILURE")
                .tenantId(tenantId).correlationId(correlationId).path(path)
                .result("FAILURE")
                .field("reason", reason)
                .build());
    }

    @Override
    public void logLogout(String userId, String tenantId, String correlationId, String appId) {
        logEvent(AuditEvent.of("LOGOUT")
                .userId(userId).tenantId(tenantId).correlationId(correlationId).appId(appId)
                .result("SUCCESS").build());
    }

    @Override
    public void logAuthorizationDenied(String userId, String tenantId, String correlationId,
                                        String path, List<String> requiredRoles) {
        logEvent(AuditEvent.of("AUTHZ_DENIED")
                .userId(userId).tenantId(tenantId).correlationId(correlationId).path(path)
                .result("DENIED")
                .field("requiredRoles", requiredRoles)
                .build());
    }

    @Override
    public void logAuthorizationGranted(String userId, String tenantId, String correlationId,
                                         String path, List<String> grantedRoles) {
        logEvent(AuditEvent.of("AUTHZ_GRANTED")
                .userId(userId).tenantId(tenantId).correlationId(correlationId).path(path)
                .result("SUCCESS")
                .field("grantedRoles", grantedRoles)
                .build());
    }

    @Override
    public void logSessionCreated(String sessionId, String tenantId, String correlationId) {
        logEvent(AuditEvent.of("SESSION_CREATED")
                .tenantId(tenantId).correlationId(correlationId)
                .result("SUCCESS")
                .field("session", maskSessionId(sessionId))
                .build());
    }

    @Override
    public void logSessionDestroyed(String sessionId, String tenantId, String correlationId, String reason) {
        logEvent(AuditEvent.of("SESSION_DESTROYED")
                .tenantId(tenantId).correlationId(correlationId)
                .result("SUCCESS")
                .field("session", maskSessionId(sessionId))
                .field("reason", reason)
                .build());
    }

    @Override
    public void logTenantSimulatorActivated(String simulatedTenantId, String correlationId) {
        logEvent(AuditEvent.of("TENANT_SIMULATOR_ACTIVE")
                .tenantId(simulatedTenantId).correlationId(correlationId)
                .result("ACTIVE")
                .field("warning", "tenant simulator is enabled; disable in production")
                .build());
    }

    @Override
    public void logStartup(String version) {
        logEvent(AuditEvent.of("SYSTEM_STARTUP")
                .result("INFO")
                .field("version", version)
                .build());
    }

    @Override
    public void logShutdown() {
        logEvent(AuditEvent.of("SYSTEM_SHUTDOWN").result("INFO").build());
    }

    @Override
    public void logApiExecution(String userId, String tenantId, String correlationId,
                                 String path, int statusCode) {
        String result = (statusCode >= 200 && statusCode < 400) ? "SUCCESS" : "ERROR";
        logEvent(AuditEvent.of("API_EXEC")
                .userId(userId).tenantId(tenantId).correlationId(correlationId).path(path)
                .result(result)
                .field("statusCode", statusCode)
                .build());
    }

    @Override
    public void logDecryptionFailure(String context) {
        logEvent(AuditEvent.of("DECRYPT_FAILURE")
                .correlationId(mdcCorrelation()).path(context)
                .result("FAILURE")
                .field("detail", "Decryption error; possible key mismatch or data tampering")
                .build());
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private static Set<String> parseDisabledEventTypes(Map<String, Object> parameters) {
        if (parameters == null) {
            return Set.of();
        }
        Object value = parameters.get(PARAM_DISABLED_EVENT_TYPES);
        if (value instanceof List<?> list) {
            Set<String> result = new LinkedHashSet<>();
            for (Object item : list) {
                if (item != null && !item.toString().isBlank()) {
                    result.add(item.toString());
                }
            }
            return result;
        }
        return Set.of();
    }

    private static int intOrDefault(Object value, int defaultValue) {
        if (value instanceof Number n) {
            return n.intValue();
        }
        if (value instanceof String s && !s.isBlank()) {
            try {
                return Integer.parseInt(s.trim());
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    private static String blankToNull(String value) {
        return (value == null || value.isBlank() || "-".equals(value)) ? null : value;
    }

    private static String maskSessionId(String sessionId) {
        if (sessionId == null || sessionId.length() <= 8) {
            return "****";
        }
        return sessionId.substring(0, 8) + "****";
    }

    private static String mdcCorrelation() {
        String corr = MDC.get("correlationId");
        return corr != null ? corr : "-";
    }
}
