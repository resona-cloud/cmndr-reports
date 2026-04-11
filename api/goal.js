const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const HEADERS = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, PATCH, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const client = req.query.client || 'peak-flow';
  const action = req.query.action;

  // ── Onboarding state (merged from /api/onboarding) ────────────
  if (action === 'onboarding') {
    if (req.method === 'GET') {
      const r = await fetch(`${SB_URL}/rest/v1/client_onboarding_state?client_id=eq.${client}&select=*`, { headers: HEADERS });
      const rows = await r.json();
      return res.status(200).json({ state: Array.isArray(rows) ? rows[0] || null : null });
    }
    if (req.method === 'PATCH') {
      const r = await fetch(`${SB_URL}/rest/v1/client_onboarding_state`, {
        method: 'POST',
        headers: { ...HEADERS, Prefer: 'resolution=merge-duplicates,return=minimal' },
        body: JSON.stringify({ client_id: client, welcome_dismissed: true, updated_at: new Date().toISOString() })
      });
      return res.status(r.ok ? 200 : 500).json({ ok: r.ok });
    }
  }

  // ── Goal ──────────────────────────────────────────────────────
  if (req.method === 'GET') {
    try {
      const r = await fetch(`${SB_URL}/rest/v1/client_goals?client_id=eq.${client}`, { headers: HEADERS });
      const rows = await r.json();
      return res.status(200).json({ goal: Array.isArray(rows) ? rows[0] || null : null });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  if (req.method === 'PATCH') {
    const body = req.body || {};
    if (!body.stated_goal) return res.status(400).json({ error: 'stated_goal is required' });
    try {
      await fetch(`${SB_URL}/rest/v1/client_goals`, {
        method: 'POST',
        headers: { ...HEADERS, Prefer: 'resolution=merge-duplicates,return=minimal' },
        body: JSON.stringify({
          client_id: client,
          stated_goal: body.stated_goal,
          goal_context: body.goal_context || null,
          set_by: body.set_by || 'resona',
          updated_at: new Date().toISOString()
        })
      });
      return res.status(200).json({ ok: true });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
