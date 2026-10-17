package ma.s2m.payos.audit.database;

import ma.s2m.payos.config.PayOSConfig;
import ma.s2m.payos.database.IDatabaseService;
import ma.s2m.payos.events.audit.AuditEvent;
import ma.s2m.payos.events.audit.store.IAuditTrailStore;
import ma.s2m.payos.events.metrics.MetricSample;
import ma.s2m.payos.events.metrics.MetricsRecorder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Background exporter/purger for {@code payos_audit_event} — this module's counterpart to {@code
 * BufferedAuditLogger}'s own {@code Writer}, except it reads already-committed database rows
 * instead of draining an in-memory buffer (see {@link DatabaseAuditLogger}'s class Javadoc for why
 * every event here is written synchronously up front rather than buffered).
 *
 * <p>Each cycle, for every known tenant and the platform database: exports every row with a {@code
 * null exportedAt} to the bound {@link IAuditTrailStore}, oldest {@code createdAt} first — this
 * ordering is what keeps {@code FilesystemAuditTrailStore}'s hash-chain meaningful, since the chain
 * is only ever computed in append order, never re-verified against some externally stored
 * original hash. Then purges already-exported rows older than {@code retentionDays}.
 *
 * <p>An export or purge failure for one row/tenant/cycle is logged and metered, never fatal to this
 * daemon thread or to any other tenant's processing in the same cycle — the row is simply left with
 * {@code exportedAt} still {@code null} and retried on the next cycle.
 */
final class Writer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Writer.class);
    private static final int EXPORT_BATCH_SIZE = 500;

    private final IAuditTrailStore store;
    private final int retentionDays;
    private final int pollIntervalSeconds;

    Writer(IAuditTrailStore store, int retentionDays, int pollIntervalSeconds) {
        this.store = store;
        this.retentionDays = retentionDays;
        this.pollIntervalSeconds = pollIntervalSeconds;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                runOneCycle();
            } catch (RuntimeException e) {
                logger.error("payos-database-audit-trail: writer cycle failed: {}", e.toString(), e);
            }
            try {
                TimeUnit.SECONDS.sleep(pollIntervalSeconds);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** Package-private (not {@code private}) so tests can drive one deterministic cycle directly, without sleeping. */
    void runOneCycle() {
        IDatabaseService multiTenant = PayOSConfig.getDatabaseService();
        if (multiTenant != null) {
            for (String tenantId : DatabaseAuditLogger.knownTenantIds()) {
                runOneCycleForTenant(multiTenant, tenantId);
            }
        }
        IDatabaseService platform = PayOSConfig.getPlatformDatabaseService();
        if (platform != null) {
            runOneCycleForDatabase(platform, null);
        }
    }

    private void runOneCycleForTenant(IDatabaseService multiTenant, String tenantId) {
        multiTenant.setCurrentTenant(tenantId);
        try {
            runOneCycleForDatabase(multiTenant, tenantId);
        } finally {
            multiTenant.clearCurrentTenant();
        }
    }

    private void runOneCycleForDatabase(IDatabaseService db, String tenantId) {
        exportPending(db, tenantId);
        purgeExported(db, tenantId);
    }

    @SuppressWarnings("unchecked")
    private void exportPending(IDatabaseService db, String tenantId) {
        if (store == null) {
            return;
        }
        List<?> rows = db.list(
                "from PayosAuditEvent e where e.exportedAt is null order by e.createdAt asc",
                db.newParams(), EXPORT_BATCH_SIZE);
        for (Object rowObj : rows) {
            Map<String, Object> row = (Map<String, Object>) rowObj;
            AuditEvent event = AuditEventRow.toAuditEvent(row);
            try {
                store.append(event);
                row.put("exportedAt", new Date());
                db.update(DatabaseAuditLogger.ENTITY_NAME, row);
            } catch (RuntimeException e) {
                logger.warn("payos-database-audit-trail: failed to export eventId={} tenantId={}: {}",
                        event.getEventId(), tenantId, e.toString());
                MetricsRecorder.record(new MetricSample("audit.writer.export.failed", null,
                        MetricSample.Type.COUNTER, 1, "events", tenantId, event.getCorrelationId(), Map.of()));
            }
        }
    }

    private void purgeExported(IDatabaseService db, String tenantId) {
        if (retentionDays <= 0) {
            return;
        }
        Map<String, Object> params = db.newParams();
        params.put("cutoff", Date.from(Instant.now().minus(retentionDays, ChronoUnit.DAYS)));
        int purged = db.executeUpdate(
                "delete from PayosAuditEvent e where e.exportedAt is not null and e.createdAt < :cutoff",
                params);
        if (purged > 0) {
            MetricsRecorder.record(new MetricSample("audit.writer.purged", null, MetricSample.Type.COUNTER,
                    purged, "events", tenantId, null, Map.of()));
        }
    }
}
