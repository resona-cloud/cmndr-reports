const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const HEADERS = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const { client, id, action } = req.query;

  // ── GET: fetch client notifications ──────────────────────────────────
  if (req.method === 'GET') {
    if (!client) return res.status(400).json({ error: 'client required' });
    const r = await fetch(
      `${SB_URL}/rest/v1/client_notifications?client_id=eq.${client}&order=created_at.desc&limit=50&select=*`,
      { headers: HEADERS }
    );
    const rows = await r.json();
    return res.status(200).json({ notifications: Array.isArray(rows) ? rows : [] });
  }

  // ── POST: insert notification (Make webhook receiver — no auth) ───────
  if (req.method === 'POST') {
    const body = req.body || {};
    if (!body.client_id) return res.status(400).json({ error: 'client_id required' });
    const r = await fetch(`${SB_URL}/rest/v1/client_notifications`, {
      method: 'POST',
      headers: { ...HEADERS, Prefer: 'return=minimal' },
      body: JSON.stringify({
        client_id: body.client_id,
        type: body.type || 'info',
        title: body.title || '',
        body: body.body || body.message || '',
        action_url: body.action_url || null,
        read: false
      })
    });
    return res.status(r.ok ? 201 : 500).json({ ok: r.ok });
  }

  // ── PATCH: mark read ──────────────────────────────────────────────────
  if (req.method === 'PATCH') {
    if (action === 'mark_all_read' && client) {
      await fetch(
        `${SB_URL}/rest/v1/client_notifications?client_id=eq.${client}&read=eq.false`,
        { method: 'PATCH', headers: { ...HEADERS, Prefer: 'return=minimal' }, body: JSON.stringify({ read: true }) }
      );
      return res.status(200).json({ ok: true });
    }
    if (id) {
      await fetch(
        `${SB_URL}/rest/v1/client_notifications?id=eq.${id}`,
        { method: 'PATCH', headers: { ...HEADERS, Prefer: 'return=minimal' }, body: JSON.stringify({ read: true }) }
      );
      return res.status(200).json({ ok: true });
    }
    return res.status(400).json({ error: 'id or action=mark_all_read required' });
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
