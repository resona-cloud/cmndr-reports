const SB_URL = process.env.SUPABASE_URL;
const SB_ANON = process.env.SUPABASE_ANON_KEY;
const SB_SVC = process.env.SUPABASE_SERVICE_ROLE_KEY;

const SVC_HEADERS = { apikey: SB_SVC, Authorization: `Bearer ${SB_SVC}`, 'Content-Type': 'application/json' };

async function getProfile(userId) {
  const r = await fetch(`${SB_URL}/rest/v1/user_profiles?id=eq.${userId}&select=*`, {
    headers: SVC_HEADERS
  });
  const rows = await r.json();
  return Array.isArray(rows) ? rows[0] || null : null;
}

async function getBranding(clientId) {
  if (!clientId) return null;
  try {
    const r = await fetch(`${SB_URL}/rest/v1/client_branding?client_id=eq.${clientId}&select=*`, {
      headers: SVC_HEADERS
    });
    const rows = await r.json();
    return Array.isArray(rows) ? rows[0] || null : null;
  } catch { return null; }
}

async function verifyResona(token) {
  if (!token) return null;
  try {
    const userRes = await fetch(`${SB_URL}/auth/v1/user`, {
      headers: { apikey: SB_ANON, Authorization: `Bearer ${token}` }
    });
    if (!userRes.ok) return null;
    const user = await userRes.json();
    const profile = await getProfile(user.id);
    if (!profile?.role?.startsWith('resona_')) return null;
    return { user, profile };
  } catch { return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, PATCH, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action;

  // ── GET ?action=branding&client=X ─────────────────────────────────────
  if (action === 'branding' && req.method === 'GET') {
    const client = req.query.client;
    if (!client) return res.status(400).json({ error: 'client required' });
    const branding = await getBranding(client);
    return res.status(200).json({ branding });
  }

  // ── PATCH ?action=branding&client=X (resona only) ─────────────────────
  if (action === 'branding' && req.method === 'PATCH') {
    const token = (req.headers.authorization || '').replace('Bearer ', '');
    if (!(await verifyResona(token))) return res.status(403).json({ error: 'Forbidden' });
    const client = req.query.client;
    if (!client) return res.status(400).json({ error: 'client required' });
    const body = req.body || {};
    const r = await fetch(`${SB_URL}/rest/v1/client_branding`, {
      method: 'POST',
      headers: { ...SVC_HEADERS, Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: JSON.stringify({
        client_id: client,
        ...(body.accent_color !== undefined && { accent_color: body.accent_color }),
        ...(body.secondary_color !== undefined && { secondary_color: body.secondary_color }),
        ...(body.company_name !== undefined && { company_name: body.company_name }),
        ...(body.logo_url !== undefined && { logo_url: body.logo_url }),
        updated_at: new Date().toISOString()
      })
    });
    return res.status(r.ok ? 200 : 500).json({ ok: r.ok });
  }

  // ── POST ?action=login ─────────────────────────────────────────────────
  if (action === 'login' && req.method === 'POST') {
    const { email, password } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: 'email and password required' });

    const authRes = await fetch(`${SB_URL}/auth/v1/token?grant_type=password`, {
      method: 'POST',
      headers: { apikey: SB_ANON, 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password })
    });
    const authData = await authRes.json();

    if (!authRes.ok || !authData.access_token) {
      return res.status(401).json({ error: authData.error_description || authData.msg || 'Invalid credentials' });
    }

    const profile = await getProfile(authData.user.id);
    if (!profile) return res.status(403).json({ error: 'No profile found for this user' });

    const branding = await getBranding(profile.client_id);

    return res.status(200).json({
      session: { access_token: authData.access_token, expires_in: authData.expires_in },
      user: { id: authData.user.id, email: authData.user.email },
      profile,
      branding
    });
  }

  // ── POST ?action=logout ────────────────────────────────────────────────
  if (action === 'logout' && req.method === 'POST') {
    const token = (req.headers.authorization || '').replace('Bearer ', '');
    if (token) {
      await fetch(`${SB_URL}/auth/v1/logout`, {
        method: 'POST',
        headers: { apikey: SB_ANON, Authorization: `Bearer ${token}` }
      });
    }
    return res.status(200).json({ ok: true });
  }

  // ── GET ?action=me ─────────────────────────────────────────────────────
  if (action === 'me' && req.method === 'GET') {
    const token = (req.headers.authorization || '').replace('Bearer ', '');
    if (!token) return res.status(401).json({ error: 'No token' });

    const userRes = await fetch(`${SB_URL}/auth/v1/user`, {
      headers: { apikey: SB_ANON, Authorization: `Bearer ${token}` }
    });
    if (!userRes.ok) return res.status(401).json({ error: 'Invalid or expired token' });

    const user = await userRes.json();
    const profile = await getProfile(user.id);
    if (!profile) return res.status(403).json({ error: 'No profile found' });

    const branding = await getBranding(profile.client_id);

    return res.status(200).json({ user: { id: user.id, email: user.email }, profile, branding });
  }

  return res.status(400).json({ error: 'Invalid action' });
};
