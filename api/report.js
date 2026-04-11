const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;

async function query(table, params) {
  const res = await fetch(`${SB_URL}/rest/v1/${table}?${params}`, {
    headers: {
      apikey: SB_KEY,
      Authorization: `Bearer ${SB_KEY}`,
      'Content-Type': 'application/json',
    },
  });
  return res.json();
}

function dateRange(preset) {
  const now = new Date();
  const today = now.toISOString().split('T')[0];
  switch(preset) {
    case '7d': {
      const d = new Date(); d.setDate(d.getDate() - 7);
      return { from: d.toISOString().split('T')[0], to: today };
    }
    case '30d': {
      const d = new Date(); d.setDate(d.getDate() - 30);
      return { from: d.toISOString().split('T')[0], to: today };
    }
    case 'mtd': {
      const d = new Date(now.getFullYear(), now.getMonth(), 1);
      return { from: d.toISOString().split('T')[0], to: today };
    }
    case 'qtd': {
      const q = Math.floor(now.getMonth() / 3);
      const d = new Date(now.getFullYear(), q * 3, 1);
      return { from: d.toISOString().split('T')[0], to: today };
    }
    default: {
      const d = new Date(); d.setDate(d.getDate() - 7);
      return { from: d.toISOString().split('T')[0], to: today };
    }
  }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const client = req.query.client || 'peak-flow';
  const preset = req.query.range || '7d';
  const from = req.query.from || dateRange(preset).from;
  const to = req.query.to || dateRange(preset).to;

  const [jobs, pipeline, technicians, alerts] = await Promise.all([
    query('jobs', `client_id=eq.${client}&scheduled_date=gte.${from}&scheduled_date=lte.${to}&order=scheduled_date.desc`),
    query('pipeline_stages', `client_id=eq.${client}&order=week_label.asc`),
    query('technicians', `client_id=eq.${client}&order=utilization_pct.desc`),
    query('alerts', `client_id=eq.${client}&order=severity.desc`),
  ]);

  const completed = jobs.filter(j => j.status === 'completed').length;
  const inprog = jobs.filter(j => j.status === 'in-progress').length;
  const weekRev = jobs
    .filter(j => j.completed_date)
    .reduce((a, j) => a + (j.revenue || 0), 0);
  const activeAlerts = alerts.filter(a => !a.resolved);
  const topAlert = activeAlerts[0]?.message || 'none';

  let summary = 'AI summary unavailable.';
  try {
    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 200,
        messages: [{ role: 'user', content: `3-sentence operational briefing for Peak Flow Plumbing & HVAC for the period ${from} to ${to}. Plain text only. Data: ${completed} completed, ${inprog} in progress. Revenue: $${Math.round(weekRev).toLocaleString()}. Top alert: "${topAlert}"` }],
      }),
    });
    const aiData = await aiRes.json();
    summary = aiData.content?.find(b => b.type === 'text')?.text || summary;
  } catch(e) {}

  res.status(200).json({ jobs, pipeline, technicians, alerts, summary, from, to, preset, generatedAt: new Date().toISOString() });
};
