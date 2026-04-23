const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const HEADERS = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };

async function sbFetch(table, params = '') {
  const r = await fetch(
    `${SB_URL}/rest/v1/${table}?${params}`,
    { headers: HEADERS }
  );
  const j = await r.json();
  return Array.isArray(j) ? j : [];
}

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

async function generateExtendedReportAsync(id, clientId, from, to, sections) {
  try {
    const [snaps, metrics, roadmap, goal] = await Promise.all([
      sbFetch('report_snapshots', `client_id=eq.${clientId}&order=created_at.desc&limit=20`),
      sbFetch('power_metric_entries', `client_id=eq.${clientId}&order=period_month.desc&limit=20`),
      sbFetch('roadmaps', `client_id=eq.${clientId}&limit=1`),
      sbFetch('client_goals', `client_id=eq.${clientId}&limit=1`)
    ]);

    const snapSummaries = snaps.slice(0, 4).map(s =>
      `${s.page}: ${s.ai_summary || 'No summary'}`
    ).join('\n');

    const execPrompt = `You are writing an executive summary for a business intelligence report.
Client goal: ${goal[0]?.stated_goal || 'Not specified'}
Section summaries:
${snapSummaries}

Write a 3-4 sentence executive summary in plain text. Be specific and data-driven.`;

    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 300,
        messages: [{ role: 'user', content: execPrompt }]
      })
    });
    const aiData = await aiRes.json();
    const execSummary = aiData.content?.find(b => b.type === 'text')?.text || '';

    await fetch(`${SB_URL}/rest/v1/extended_reports?id=eq.${id}`, {
      method: 'PATCH',
      headers: { ...HEADERS, Prefer: 'return=minimal' },
      body: JSON.stringify({
        status: 'complete',
        executive_summary: execSummary,
        sections: {
          operations: snaps.find(s => s.page === 'operations') || null,
          marketing: snaps.find(s => s.page === 'marketing') || null,
          finance: snaps.find(s => s.page === 'finance') || null,
          optimization: snaps.find(s => s.page === 'optimization') || null
        },
        power_metrics: metrics,
        generated_at: new Date().toISOString()
      })
    });
  } catch(e) {
    console.error('[extended-report]', e.message);
    await fetch(`${SB_URL}/rest/v1/extended_reports?id=eq.${id}`, {
      method: 'PATCH',
      headers: { ...HEADERS, Prefer: 'return=minimal' },
      body: JSON.stringify({ status: 'error' })
    });
  }
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

  // ── GET: Hub clients roster ───────────────────────────────────────────
  if (req.method === 'GET' && action === 'hub-clients') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    try {
      const [profiles, health, goals, sources, courses] = await Promise.all([
        sbFetch('user_profiles', 'role=eq.client_user&select=*'),
        sbFetch('client_health_scores', 'select=client_id,score,score_label'),
        sbFetch('client_goals', 'select=client_id,stated_goal'),
        sbFetch('data_sources', 'connected=eq.true&select=client_id,connector_key'),
        sbFetch('client_courses', 'select=client_id,course_display,current_milestone_index,milestones')
      ]);

      const healthMap = {};
      health.forEach(h => { healthMap[h.client_id] = h; });
      const goalMap = {};
      goals.forEach(g => { goalMap[g.client_id] = g; });
      const connMap = {};
      sources.forEach(s => { connMap[s.client_id] = (connMap[s.client_id] || 0) + 1; });
      const courseMap = {};
      courses.forEach(c => { courseMap[c.client_id] = c; });

      const seen = new Set();
      const clients = [];
      for (const p of profiles) {
        const cid = p.client_id;
        if (!cid || seen.has(cid)) continue;
        seen.add(cid);
        const h = healthMap[cid];
        const g = goalMap[cid];
        const c = courseMap[cid];
        const milestones = c?.milestones || [];
        const idx = c?.current_milestone_index || 0;
        clients.push({
          client_id: cid,
          full_name: p.full_name || cid,
          stated_goal: g?.stated_goal || null,
          health_score: h?.score ?? null,
          health_label: h?.score_label || null,
          connected_count: connMap[cid] || 0,
          current_milestone: milestones[idx]?.display || null,
          course_display: c?.course_display || null
        });
      }
      return res.status(200).json({ clients });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET: Hub client detail ────────────────────────────────────────────
  if (req.method === 'GET' && action === 'hub-client-detail') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const clientId = req.query.client_id;
    if (!clientId) return res.status(400).json({ error: 'client_id required' });
    try {
      const [profiles, health, goal, course, roadmap, keywords, sources] = await Promise.all([
        sbFetch('user_profiles', `client_id=eq.${clientId}&limit=1`),
        sbFetch('client_health_scores', `client_id=eq.${clientId}&limit=1`),
        sbFetch('client_goals', `client_id=eq.${clientId}&limit=1`),
        sbFetch('client_courses', `client_id=eq.${clientId}&limit=1`),
        sbFetch('roadmaps', `client_id=eq.${clientId}&limit=1`),
        sbFetch('client_keywords', `client_id=eq.${clientId}&limit=1`),
        sbFetch('data_sources', `client_id=eq.${clientId}&connected=eq.true`)
      ]);
      return res.status(200).json({
        profile: profiles[0] || null,
        health: health[0] || null,
        goal: goal[0] || null,
        course: course[0] || null,
        roadmap: roadmap[0] || null,
        keywords: keywords[0] || null,
        sources
      });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET: Hub archive ──────────────────────────────────────────────────
  if (req.method === 'GET' && action === 'hub-archive') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const clientId = req.query.client_id;
    if (!clientId) return res.status(400).json({ error: 'client_id required' });
    try {
      const snaps = await sbFetch('report_snapshots',
        `client_id=eq.${clientId}&order=created_at.desc&limit=20&select=id,page,period_label,period_from,period_to,created_at,ai_summary`
      );
      return res.status(200).json({ snapshots: snaps });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── POST: Hub save keywords ───────────────────────────────────────────
  if (req.method === 'POST' && action === 'hub-save-keywords') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const body = req.body || {};
    if (!body.client_id) return res.status(400).json({ error: 'client_id required' });
    try {
      await fetch(`${SB_URL}/rest/v1/client_keywords`, {
        method: 'POST',
        headers: { ...HEADERS, Prefer: 'resolution=merge-duplicates,return=minimal' },
        body: JSON.stringify({
          client_id: body.client_id,
          keywords: body.keywords,
          updated_at: new Date().toISOString()
        })
      });
      return res.status(200).json({ success: true });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── POST: Hub advance milestone ───────────────────────────────────────
  if (req.method === 'POST' && action === 'hub-advance-milestone') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const body = req.body || {};
    if (!body.client_id) return res.status(400).json({ error: 'client_id required' });
    try {
      await fetch(
        `${SB_URL}/rest/v1/roadmaps?client_id=eq.${body.client_id}`,
        {
          method: 'PATCH',
          headers: { ...HEADERS, Prefer: 'return=minimal' },
          body: JSON.stringify({ current_node: body.current_node + 1 })
        }
      );
      return res.status(200).json({ success: true, new_node: body.current_node + 1 });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── POST: Hub create client ───────────────────────────────────────────
  if (req.method === 'POST' && action === 'hub-create-client') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const body = req.body || {};
    if (!body.email || !body.client_id) return res.status(400).json({ error: 'email and client_id required' });
    try {
      const BASE = process.env.VERCEL_URL
        ? 'https://' + process.env.VERCEL_URL
        : 'http://localhost:3000';
      const onboardRes = await fetch(`${BASE}/api/onboard`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email: body.email,
          password: body.password,
          full_name: body.full_name,
          client_id: body.client_id,
          role: 'client_user',
          stated_goal: body.stated_goal
        })
      });
      const onboardData = await onboardRes.json();
      if (!onboardData.ok) throw new Error(onboardData.error || 'Onboard failed');

      if (body.milestone_1) {
        await fetch(`${SB_URL}/rest/v1/roadmaps`, {
          method: 'POST',
          headers: { ...HEADERS, Prefer: 'return=minimal' },
          body: JSON.stringify({
            client_id: body.client_id,
            current_node: 1,
            milestone_1: body.milestone_1 || null,
            milestone_2: body.milestone_2 || null,
            milestone_3: body.milestone_3 || null
          })
        });
      }

      if (body.keywords && body.keywords.length) {
        await fetch(`${SB_URL}/rest/v1/client_keywords`, {
          method: 'POST',
          headers: { ...HEADERS, Prefer: 'return=minimal' },
          body: JSON.stringify({
            client_id: body.client_id,
            keywords: body.keywords
          })
        });
      }

      return res.status(200).json({ success: true, client_id: body.client_id });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET: Hub power metrics ────────────────────────────────────────────
  if (req.method === 'GET' && action === 'hub-power-metrics') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const client_id = req.query.client_id;
    if (!client_id) return res.status(400).json({ error: 'client_id required' });
    try {
      const configs = await sbFetch('client_power_metrics',
        `client_id=eq.${client_id}&is_active=eq.true&order=display_order.asc`
      );
      const defs = await sbFetch('power_metric_definitions', '');
      const defMap = {};
      defs.forEach(d => { defMap[d.key] = d; });
      const metrics = configs.map(c => ({
        ...defMap[c.key],
        target_value: c.target_value,
        target_label: c.target_label,
        display_order: c.display_order
      }));
      return res.status(200).json({ metrics });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET: Hub metric history ───────────────────────────────────────────
  if (req.method === 'GET' && action === 'hub-metric-history') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const client_id = req.query.client_id;
    if (!client_id) return res.status(400).json({ error: 'client_id required' });
    try {
      const entries = await sbFetch('power_metric_entries',
        `client_id=eq.${client_id}&order=period_month.desc&limit=50`
      );
      const defs = await sbFetch('power_metric_definitions', '');
      const defMap = {};
      defs.forEach(d => { defMap[d.key] = d; });
      const enriched = entries.map(e => ({
        ...e,
        metric_label: defMap[e.metric_key]?.label || e.metric_key,
        unit: defMap[e.metric_key]?.unit || 'number'
      }));
      return res.status(200).json({ entries: enriched });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── POST: Hub log metric ──────────────────────────────────────────────
  if (req.method === 'POST' && action === 'hub-log-metric') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const body = req.body || {};
    try {
      await fetch(`${SB_URL}/rest/v1/power_metric_entries`, {
        method: 'POST',
        headers: { ...HEADERS, Prefer: 'return=minimal' },
        body: JSON.stringify({
          client_id: body.client_id,
          metric_key: body.metric_key,
          value: parseFloat(body.value),
          period_label: body.period_label,
          period_month: body.period_month,
          notes: body.notes || null,
          entered_by: 'advisor',
          source: 'manual'
        })
      });
      return res.status(200).json({ success: true });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── POST: Hub save quick report ───────────────────────────────────────
  if (req.method === 'POST' && action === 'hub-save-quick-report') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const b = req.body || {};
    try {
      await fetch(`${SB_URL}/rest/v1/quick_reports`, {
        method: 'POST',
        headers: { ...HEADERS, Prefer: 'return=minimal' },
        body: JSON.stringify({
          client_id: b.client_id,
          title: b.title,
          report_type: b.report_type || 'note',
          priority: b.priority || 'normal',
          body: b.body,
          linked_metric: b.linked_metric || null,
          linked_milestone: b.linked_milestone || null,
          visible_to_client: b.visible_to_client || false,
          created_by: 'advisor',
          created_at: new Date().toISOString()
        })
      });
      return res.status(200).json({ success: true });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET: Hub quick reports ────────────────────────────────────────────
  if (req.method === 'GET' && action === 'hub-quick-reports') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const limit = req.query.limit || 10;
    try {
      const reports = await sbFetch('quick_reports',
        `client_id=eq.${req.query.client_id}&order=created_at.desc&limit=${limit}`
      );
      return res.status(200).json({ reports });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── POST: Hub log action ──────────────────────────────────────────────
  if (req.method === 'POST' && action === 'hub-log-action') {
    await verifyResona(req); // soft check — don't fail hard
    const b = req.body || {};
    try {
      await fetch(`${SB_URL}/rest/v1/agent_logs`, {
        method: 'POST',
        headers: { ...HEADERS, Prefer: 'return=minimal' },
        body: JSON.stringify({
          client_id: b.client_id,
          action: b.action,
          detail: b.detail || null,
          created_at: new Date().toISOString()
        })
      });
      return res.status(200).json({ success: true });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET: Hub agent logs ───────────────────────────────────────────────
  if (req.method === 'GET' && action === 'hub-agent-logs') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const limit = req.query.limit || 10;
    try {
      const logs = await sbFetch('agent_logs',
        `client_id=eq.${req.query.client_id}&order=created_at.desc&limit=${limit}`
      );
      return res.status(200).json({ logs });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET: Hub client settings ──────────────────────────────────────────
  if (req.method === 'GET' && action === 'hub-client-settings') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const client_id = req.query.client_id;
    if (!client_id) return res.status(400).json({ error: 'client_id required' });
    try {
      const [settingsRows, profileRows, snapRows, kwRows] = await Promise.all([
        sbFetch('client_settings', `client_id=eq.${client_id}&limit=1`),
        sbFetch('user_profiles', `client_id=eq.${client_id}&select=id`),
        sbFetch('report_snapshots', `client_id=eq.${client_id}&select=id`),
        sbFetch('client_keywords', `client_id=eq.${client_id}&select=id`)
      ]);
      return res.status(200).json({
        settings: settingsRows[0] || {},
        meta: {
          profile_count: profileRows.length,
          snapshot_count: snapRows.length,
          keyword_count: kwRows.length
        }
      });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── POST: Hub extended report ─────────────────────────────────────────
  if (req.method === 'POST' && action === 'hub-extended-report') {
    if (!(await verifyResona(req))) return res.status(403).json({ error: 'Forbidden' });
    const b = req.body || {};
    try {
      const id = require('crypto').randomUUID();
      await fetch(`${SB_URL}/rest/v1/extended_reports`, {
        method: 'POST',
        headers: { ...HEADERS, Prefer: 'return=minimal' },
        body: JSON.stringify({
          id,
          client_id: b.client_id,
          title: `Extended Report — ${b.from} to ${b.to}`,
          status: 'draft',
          period_from: b.from,
          period_to: b.to,
          sections: b.sections || {},
          created_by: 'advisor',
          created_at: new Date().toISOString()
        })
      });
      generateExtendedReportAsync(id, b.client_id, b.from, b.to, b.sections);
      return res.status(200).json({ success: true, report_id: id, status: 'generating' });
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
