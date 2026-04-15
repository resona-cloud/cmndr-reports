const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const HEADERS = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };

async function verifyResona(req) {
  const token = (req.headers.authorization || '').replace('Bearer ', '');
  if (!token) return false;
  try {
    const userRes = await fetch(`${SB_URL}/auth/v1/user`, {
      headers: { apikey: SB_KEY, Authorization: `Bearer ${token}` }
    });
    if (!userRes.ok) return false;
    const user = await userRes.json();
    const profRes = await fetch(`${SB_URL}/rest/v1/user_profiles?id=eq.${user.id}&select=role`, { headers: HEADERS });
    const prof = await profRes.json();
    return !!(prof[0]?.role?.startsWith('resona_'));
  } catch { return false; }
}

const EVENT_LABELS = {
  job_completed:       (d, cn) => `${cn} — Job completed: ${d.title || 'unknown'}`,
  lead_received:       (d, cn) => `${cn} — New lead from ${d.source || 'unknown source'}`,
  invoice_overdue:     (d, cn) => `${cn} — Invoice overdue: $${(d.amount || 0).toLocaleString()}`,
  automation_error:    (d, cn) => `${cn} — Automation error: ${d.scenario_name || 'unknown scenario'}`,
  health_recalculated: (d, cn) => `${cn} — Health score updated: ${d.new_score || '—'}`,
  report_opened:       (d, cn) => `${cn} — Client opened their report`,
  action_executed:     (d, cn) => `${cn} — Action executed: ${d.action || 'unknown'}`,
  milestone_check:     (d, cn) => `${cn} — Milestone activity: ${d.action || 'checked'}`,
};

function typeToColor(t) {
  if (t === 'automation_error' || t === 'invoice_overdue') return 'red';
  if (t === 'health_recalculated' || t === 'lead_received') return 'amber';
  if (t === 'job_completed' || t === 'milestone_check') return 'green';
  return 'blue';
}

function timeAgo(dateStr) {
  const diff = Date.now() - new Date(dateStr).getTime();
  const m = Math.floor(diff / 60000);
  if (m < 1) return 'just now';
  if (m < 60) return m + 'm ago';
  const h = Math.floor(m / 60);
  if (h < 24) return h + 'h ago';
  return Math.floor(h / 24) + 'd ago';
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action;

  // ── GET: Weekly report pre-generation (cron) ─────────────────────────
  if (req.method === 'GET' && action === 'weekly-report') {
    if (req.headers['x-cron-secret'] !== process.env.CRON_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const d = new Date();
    const dow = d.getUTCDay();
    d.setUTCDate(d.getUTCDate() + (dow === 0 ? -6 : 1 - dow));
    const weekStart = d.toISOString().split('T')[0];

    try {
      const profilesRes = await fetch(
        `${SB_URL}/rest/v1/user_profiles?role=eq.client_user&select=client_id`,
        { headers: HEADERS }
      );
      const profiles = await profilesRes.json();
      const clientIds = [...new Set(
        (Array.isArray(profiles) ? profiles : []).map(p => p.client_id).filter(Boolean)
      )];

      const BASE = `https://${process.env.VERCEL_PROJECT_PRODUCTION_URL || process.env.VERCEL_URL}`;
      const pages = ['operations', 'marketing', 'finance', 'optimization'];
      const results = [];

      for (const clientId of clientIds) {
        for (const page of pages) {
          try {
            await fetch(`${BASE}/api/pages?page=${page}&client=${clientId}&from=${weekStart}&to=${weekStart}`);
            results.push({ client: clientId, page, ok: true });
          } catch (e) {
            results.push({ client: clientId, page, ok: false, error: e.message });
          }
        }
      }

      return res.status(200).json({ weekStart, clients: clientIds.length, generated: results.length, results });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── POST: Make webhook sync event (no auth — secured by URL) ─────────
  if (req.method === 'POST' && action === 'sync_event') {
    const { client_id, scenario_id, scenario_name, status, duration_ms, operations_used, triggered_by } = req.body || {};
    if (!client_id || !scenario_name) return res.status(400).json({ error: 'client_id and scenario_name required' });

    try {
      // Fetch current row to increment counts
      const existRes = await fetch(
        `${SB_URL}/rest/v1/make_scenario_health?client_id=eq.${client_id}&scenario_name=eq.${encodeURIComponent(scenario_name)}&select=*`,
        { headers: HEADERS }
      );
      const existing = await existRes.json();
      const row = Array.isArray(existing) && existing[0] ? existing[0] : null;

      const totalRuns = (row?.total_runs || 0) + 1;
      const errorCount = (row?.error_count || 0) + (status === 'error' ? 1 : 0);
      const successRate = Math.round((totalRuns - errorCount) / totalRuns * 100);

      const upsertBody = {
        client_id,
        scenario_name,
        ...(scenario_id && { scenario_id }),
        status: status || 'active',
        last_run_at: new Date().toISOString(),
        total_runs: totalRuns,
        error_count: errorCount,
        success_rate: successRate,
        operations_count: operations_used || row?.operations_count || 0,
        updated_at: new Date().toISOString()
      };

      await fetch(`${SB_URL}/rest/v1/make_scenario_health`, {
        method: 'POST',
        headers: { ...HEADERS, Prefer: 'resolution=merge-duplicates,return=minimal' },
        body: JSON.stringify(upsertBody)
      });

      // If error: notify client + write live_event
      if (status === 'error') {
        fetch(`${SB_URL}/rest/v1/client_notifications`, {
          method: 'POST',
          headers: { ...HEADERS, Prefer: 'return=minimal' },
          body: JSON.stringify({
            client_id,
            type: 'alert_triggered',
            title: `Automation error: ${scenario_name}`,
            body: 'A Make scenario failed — check the Pipeline view for details',
            read: false
          })
        }).catch(() => {});

        fetch(`${SB_URL}/rest/v1/live_events`, {
          method: 'POST',
          headers: { ...HEADERS, Prefer: 'return=minimal' },
          body: JSON.stringify({
            client_id,
            event_type: 'automation_error',
            event_data: { scenario_id, scenario_name, client_id, duration_ms, triggered_by }
          })
        }).catch(() => {});
      }

      return res.status(200).json({ received: true, total_runs: totalRuns, success_rate: successRate });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET: Hub notifications (resona only) ──────────────────────────────
  if (req.method === 'GET' && action === 'hub_notifications') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });

    try {
      const eventsRes = await fetch(
        `${SB_URL}/rest/v1/live_events?order=created_at.desc&limit=20&select=*`,
        { headers: HEADERS }
      );
      const events = await eventsRes.json();
      if (!Array.isArray(events)) return res.status(200).json({ notifications: [] });

      // Get client names
      const clientIds = [...new Set(events.map(e => e.client_id).filter(Boolean))];
      let clientNames = {};
      if (clientIds.length) {
        const cnRes = await fetch(
          `${SB_URL}/rest/v1/user_profiles?client_id=in.(${clientIds.join(',')})&role=eq.client_user&select=client_id,full_name&limit=50`,
          { headers: HEADERS }
        );
        const cnRows = await cnRes.json();
        if (Array.isArray(cnRows)) {
          cnRows.forEach(r => { if (!clientNames[r.client_id]) clientNames[r.client_id] = r.full_name; });
        }
      }

      const notifications = events.map(e => {
        const cn = clientNames[e.client_id] || e.client_id || 'Unknown client';
        const labelFn = EVENT_LABELS[e.event_type];
        const data = e.event_data || {};
        const message = labelFn ? labelFn(data, cn) : `${cn} — ${e.event_type}`;
        return {
          id: e.id,
          client_id: e.client_id,
          client_name: cn,
          event_type: e.event_type,
          message,
          color: typeToColor(e.event_type),
          time_ago: timeAgo(e.created_at),
          created_at: e.created_at
        };
      });

      return res.status(200).json({ notifications });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET: Scenario health (resona only) ────────────────────────────────
  if (req.method === 'GET') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });

    const client = req.query.client;
    let url = `${SB_URL}/rest/v1/make_scenario_health?order=client_id.asc,scenario_name.asc&select=*`;
    if (client) url += `&client_id=eq.${client}`;

    try {
      const r = await fetch(url, { headers: HEADERS });
      const rows = await r.json();
      if (!Array.isArray(rows)) return res.status(500).json({ error: 'Failed to fetch pipeline data' });

      const grouped = {};
      for (const row of rows) {
        if (!grouped[row.client_id]) grouped[row.client_id] = [];
        grouped[row.client_id].push(row);
      }

      const totalRuns = rows.reduce((a, r) => a + (r.total_runs || 0), 0);
      const totalErrors = rows.reduce((a, r) => a + (r.error_count || 0), 0);
      const erroredScenarios = rows.filter(r => r.status === 'error').length;
      const activeScenarios = rows.filter(r => r.status === 'active').length;
      const lastSync = rows.length ? rows.reduce((latest, r) => {
        if (!r.last_run_at) return latest;
        return !latest || r.last_run_at > latest ? r.last_run_at : latest;
      }, null) : null;

      return res.status(200).json({
        scenarios: rows, grouped,
        summary: { totalScenarios: rows.length, activeScenarios, erroredScenarios, totalRuns, totalErrors, lastSync }
      });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
