// /api/actions — action history + webhook triggers
// /api/notifications (rewritten here via vercel.json) — client notifications

const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const HEADERS = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };

const PAGE_ACTION_TYPE = {
  operations:   'schedule_followup',
  marketing:    'send_campaign',
  finance:      'send_invoice',
  optimization: 'trigger_automation',
};

// ── Notifications handler (forwarded from /api/notifications) ──
async function handleNotifications(req, res) {
  const { client, id, action } = req.query;

  if (req.method === 'GET') {
    if (!client) return res.status(400).json({ error: 'client required' });
    const r = await fetch(
      `${SB_URL}/rest/v1/client_notifications?client_id=eq.${client}&order=created_at.desc&limit=50&select=*`,
      { headers: HEADERS }
    );
    const rows = await r.json();
    return res.status(200).json({ notifications: Array.isArray(rows) ? rows : [] });
  }

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
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Route notifications requests (forwarded via vercel.json rewrite)
  if (req.query._type === 'notifications') {
    return handleNotifications(req, res);
  }

  // ── Actions handler ────────────────────────────────────────
  if (req.method === 'GET') {
    const client = req.query.client;
    if (!client) return res.status(400).json({ error: 'client required' });
    const r = await fetch(
      `${SB_URL}/rest/v1/action_history?client_id=eq.${client}&order=decided_at.desc&limit=30&select=*`,
      { headers: HEADERS }
    );
    const rows = await r.json();
    return res.status(200).json({ actions: Array.isArray(rows) ? rows : [] });
  }

  if (req.method === 'POST') {
    const body = req.body || {};
    if (!body.client_id) return res.status(400).json({ error: 'client_id required' });

    const decidedAt = new Date().toISOString();
    const decision = body.decision || 'approved';

    const insertRes = await fetch(`${SB_URL}/rest/v1/action_history`, {
      method: 'POST',
      headers: { ...HEADERS, Prefer: 'return=representation' },
      body: JSON.stringify({
        client_id: body.client_id,
        action_title: body.action_title || 'Action',
        action_desc: body.action_desc || '',
        page: body.page || '',
        decision,
        decided_by: body.decided_by || '',
        decided_at: decidedAt
      })
    });
    const inserted = await insertRes.json();
    const actionId = Array.isArray(inserted) ? inserted[0]?.id : null;

    if (decision !== 'approved') {
      return res.status(200).json({ ok: true, executed: false });
    }

    const actionType = PAGE_ACTION_TYPE[body.page] || 'trigger_automation';
    let webhookUrl = null;
    try {
      const whRes = await fetch(
        `${SB_URL}/rest/v1/action_webhooks?client_id=eq.${body.client_id}&action_type=eq.${actionType}&active=eq.true&limit=1&select=webhook_url`,
        { headers: HEADERS }
      );
      const whRows = await whRes.json();
      webhookUrl = Array.isArray(whRows) && whRows[0] ? whRows[0].webhook_url : null;
    } catch(e) {}

    if (!webhookUrl) {
      return res.status(200).json({ ok: true, executed: false });
    }

    let webhookFired = false;
    try {
      await fetch(webhookUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          client_id: body.client_id,
          action_title: body.action_title,
          action_desc: body.action_desc || '',
          page: body.page,
          decided_by: body.decided_by,
          decided_at: decidedAt
        })
      });
      webhookFired = true;
    } catch(e) {}

    if (actionId) {
      fetch(`${SB_URL}/rest/v1/action_history?id=eq.${actionId}`, {
        method: 'PATCH',
        headers: { ...HEADERS, Prefer: 'return=minimal' },
        body: JSON.stringify({ outcome: 'Execution triggered via Make', outcome_note: 'Webhook fired to Make scenario' })
      }).catch(() => {});
    }

    fetch(`${SB_URL}/rest/v1/live_events`, {
      method: 'POST',
      headers: { ...HEADERS, Prefer: 'return=minimal' },
      body: JSON.stringify({
        client_id: body.client_id,
        event_type: 'action_executed',
        event_data: { action: body.action_title, triggered_by: body.decided_by, action_type: actionType }
      })
    }).catch(() => {});

    return res.status(200).json({ ok: true, executed: true, webhook_fired: webhookFired });
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
