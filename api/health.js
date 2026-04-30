const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;

const HEADERS = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };

async function safeQuery(table, params) {
  try {
    const r = await fetch(`${SB_URL}/rest/v1/${table}?${params}`, { headers: HEADERS });
    const j = await r.json();
    return Array.isArray(j) ? j : [];
  } catch { return []; }
}

async function claude(prompt) {
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: 'claude-haiku-4-5-20251001', max_tokens: 200, messages: [{ role: 'user', content: prompt }] })
    });
    const d = await r.json();
    return d.content?.find(b => b.type === 'text')?.text || '';
  } catch { return ''; }
}

function scoreLabel(score) {
  if (score >= 81) return 'excellent';
  if (score >= 61) return 'healthy';
  if (score >= 41) return 'stable';
  if (score >= 21) return 'at_risk';
  return 'critical';
}

function labelColor(label) {
  const map = { excellent: '#1D9E75', healthy: '#1D9E75', stable: '#EF9F27', at_risk: '#E24B4A', critical: '#E24B4A' };
  return map[label] || '#9c9a92';
}

async function calculateScore(client) {
  const today = new Date().toISOString().split('T')[0];
  const from30 = new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];

  const [jobs, invoices, leads, automation_logs, alerts] = await Promise.all([
    safeQuery('jobs', `client_id=eq.${client}&scheduled_date=gte.${from30}&scheduled_date=lte.${today}`),
    safeQuery('invoices', `client_id=eq.${client}&issued_date=gte.${from30}`),
    safeQuery('leads', `client_id=eq.${client}&created_at=gte.${from30}`),
    safeQuery('automation_logs', `client_id=eq.${client}&ran_at=gte.${from30}`),
    safeQuery('alerts', `client_id=eq.${client}&resolved=eq.false`)
  ]);

  const completed = jobs.filter(j => j.status === 'completed').length;
  const opsRate = jobs.length ? completed / jobs.length : 0.5;
  const alertPenalty = Math.min(alerts.length * 3, 10);
  const operations_score = Math.max(0, Math.round(opsRate * 25) - alertPenalty);

  const converted = leads.filter(l => l.status === 'converted').length;
  const convRate = leads.length ? converted / leads.length : 0.3;
  const marketing_score = Math.round(convRate * 25);

  const paidAmt = invoices.filter(i => i.status === 'paid').reduce((a, i) => a + Number(i.amount || 0), 0);
  const totalAmt = invoices.reduce((a, i) => a + Number(i.amount || 0), 0);
  const collRate = totalAmt ? paidAmt / totalAmt : 0.5;
  const finance_score = Math.round(collRate * 25);

  const autoSuccess = automation_logs.filter(l => l.status === 'success').length;
  const autoRate = automation_logs.length ? autoSuccess / automation_logs.length : 0.5;
  const optimization_score = Math.round(autoRate * 25);

  const score = operations_score + marketing_score + finance_score + optimization_score;
  const label = scoreLabel(score);

  const reasoning = await claude(`In 2 clear sentences, explain why this business currently has a health score of ${score}/100. Operations: ${Math.round(opsRate * 100)}% job completion (${alerts.length} active alerts). Marketing: ${Math.round(convRate * 100)}% lead conversion. Finance: ${Math.round(collRate * 100)}% collection rate. Automation: ${Math.round(autoRate * 100)}% success rate. Plain text only, speak directly to the business owner.`);

  return {
    client_id: client, score, score_label: label,
    operations_score, marketing_score, finance_score, optimization_score,
    score_reasoning: reasoning, calculated_at: new Date().toISOString(),
    dimensions: { operations: operations_score, marketing: marketing_score, finance: finance_score, optimization: optimization_score }
  };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const client = req.query.client || 'peak-flow';
  const action = req.query.action;

  // ── GET ?action=entropy ────────────────────────────────────────────────
  if (req.method === 'GET' && action === 'entropy') {
    try {
      const r = await fetch(
        `${SB_URL}/rest/v1/refinery_scan_results?select=*&order=created_at.desc&limit=1`,
        { headers: HEADERS }
      );
      if (!r.ok) return res.status(200).json({ score: null, message: 'Supabase error' });
      const rows = await r.json();
      if (!rows || !rows.length) return res.status(200).json({ score: null, message: 'No scan results found' });
      const row = rows[0];
      const scores = row.scores || {};
      return res.status(200).json({
        score: scores.composite ?? null,
        completeness: scores.completeness_score ?? null,
        variance: scores.variance_score ?? null,
        fragmentation: scores.fragmentation_score ?? null,
        enrichment: scores.enrichment_score ?? null,
        scan_id: row.scan_id,
        created_at: row.created_at
      });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── POST ?action=recalculate (called by Make nightly schedule) ────────
  if (req.method === 'POST' && action === 'recalculate') {
    try {
      const record = await calculateScore(client);
      const label = record.score_label;

      await fetch(`${SB_URL}/rest/v1/client_health_scores`, {
        method: 'POST',
        headers: { ...HEADERS, Prefer: 'return=minimal' },
        body: JSON.stringify(record)
      });

      // Write live_event (fire-and-forget)
      fetch(`${SB_URL}/rest/v1/live_events`, {
        method: 'POST',
        headers: { ...HEADERS, Prefer: 'return=minimal' },
        body: JSON.stringify({
          client_id: client,
          event_type: 'health_recalculated',
          event_data: { new_score: record.score, score_label: label, triggered_by: 'make_schedule' }
        })
      }).catch(() => {});

      return res.status(200).json({ health: { ...record, color: labelColor(label) } });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── GET (existing behavior) ────────────────────────────────────────────
  if (req.method === 'GET') {
    const recalculate = req.query.recalculate === 'true';

    if (!recalculate) {
      const stored = await safeQuery('client_health_scores', `client_id=eq.${client}&order=calculated_at.desc&limit=1`);
      if (stored.length) {
        return res.status(200).json({ health: { ...stored[0], color: labelColor(stored[0].score_label) } });
      }
    }

    try {
      const record = await calculateScore(client);
      fetch(`${SB_URL}/rest/v1/client_health_scores`, {
        method: 'POST',
        headers: { ...HEADERS, Prefer: 'return=minimal' },
        body: JSON.stringify(record)
      }).catch(() => {});
      return res.status(200).json({ health: { ...record, color: labelColor(record.score_label) } });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
