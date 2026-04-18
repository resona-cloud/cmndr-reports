// /api/connections?action=<action>
// Actions: list, save-key, test, disconnect, upload, map-columns
// All connection management in one serverless function

const { createCipheriv, createDecipheriv, randomBytes } = require('crypto');

const SB_URL = process.env.SUPABASE_URL;
const SB_SVC = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ENC_KEY = process.env.ENCRYPTION_KEY; // 64-char hex = 32 bytes
const SVC_H = {
  apikey: SB_SVC,
  Authorization: `Bearer ${SB_SVC}`,
  'Content-Type': 'application/json',
};
const UPSERT_H = { ...SVC_H, Prefer: 'resolution=merge-duplicates' };
const MIN_H = { ...SVC_H, Prefer: 'return=minimal' };

// ── Crypto ────────────────────────────────────────────────────
function encrypt(plaintext) {
  const key = Buffer.from(ENC_KEY, 'hex');
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', key, iv);
  const enc = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
  return iv.toString('hex') + ':' + cipher.getAuthTag().toString('hex') + ':' + enc.toString('hex');
}

function decrypt(ciphertext) {
  const [ivH, tagH, encH] = ciphertext.split(':');
  const key = Buffer.from(ENC_KEY, 'hex');
  const decipher = createDecipheriv('aes-256-gcm', key, Buffer.from(ivH, 'hex'));
  decipher.setAuthTag(Buffer.from(tagH, 'hex'));
  const enc = Buffer.from(encH, 'hex');
  return decipher.update(enc, undefined, 'utf8') + decipher.final('utf8');
}

// ── Supabase helpers ──────────────────────────────────────────
async function sbGet(table, query) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}?${query}`, { headers: SVC_H });
  if (!r.ok) return [];
  return r.json();
}

// ── Default catalog ───────────────────────────────────────────
const DEFAULT_CATALOG = [
  { id: 'servicetitan', name: 'ServiceTitan', category: 'Field Management', icon: '🔧', description: 'Jobs, technicians, invoices, and revenue data', auth_type: 'api_key' },
  { id: 'jobber', name: 'Jobber', category: 'Field Management', icon: '🏠', description: 'Quoting, scheduling, and job tracking', auth_type: 'api_key' },
  { id: 'housecall_pro', name: 'HouseCall Pro', category: 'Field Management', icon: '⚡', description: 'Dispatch, payments, and customer records', auth_type: 'api_key' },
  { id: 'google_ads', name: 'Google Ads', category: 'Marketing & CRM', icon: '📣', description: 'Ad spend, impressions, and lead attribution', auth_type: 'oauth' },
  { id: 'google_business', name: 'Google Business Profile', category: 'Marketing & CRM', icon: '💬', description: 'Reviews, calls, and local search performance', auth_type: 'oauth' },
  { id: 'quickbooks', name: 'QuickBooks', category: 'Finance', icon: '📊', description: 'P&L, invoices, expenses, and payroll', auth_type: 'oauth' },
  { id: 'stripe', name: 'Stripe', category: 'Finance', icon: '💳', description: 'Payment processing and revenue analytics', auth_type: 'api_key' },
  { id: 'mailchimp', name: 'Mailchimp', category: 'Marketing & CRM', icon: '✉️', description: 'Email campaigns and audience analytics', auth_type: 'api_key' },
  { id: 'airtable', name: 'Airtable', category: 'Data Import', icon: '📋', description: 'Connect Airtable bases as a data source', auth_type: 'api_key' },
  { id: 'csv_upload', name: 'CSV / Excel Upload', category: 'Data Import', icon: '📁', description: 'Upload spreadsheet files directly', auth_type: 'upload' },
];

// ── GSC sync ──────────────────────────────────────────────────
async function syncGSCData(client_id, accessToken) {
  const SB_URL = process.env.SUPABASE_URL;
  const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
  const HEADERS = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };

  const sessionRes = await fetch(
    `${SB_URL}/rest/v1/audit_sessions?client_id=eq.${client_id}&order=created_at.desc&limit=1&select=domain`,
    { headers: HEADERS }
  );
  const sessions = await sessionRes.json();
  const siteUrl = Array.isArray(sessions) && sessions[0]?.domain ? sessions[0].domain : null;

  if (!siteUrl) {
    console.log('[gsc-sync] No domain found for client', client_id);
    return 0;
  }

  const endDate = new Date().toISOString().split('T')[0];
  const startDate = new Date(Date.now() - 28 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

  const gscRes = await fetch(
    `https://www.googleapis.com/webmasters/v3/sites/${encodeURIComponent(siteUrl)}/searchAnalytics/query`,
    {
      method: 'POST',
      headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ startDate, endDate, dimensions: ['query', 'page'], rowLimit: 50, startRow: 0 })
    }
  );
  const gscData = await gscRes.json();
  const rows = gscData.rows || [];
  if (!rows.length) return 0;

  await fetch(`${SB_URL}/rest/v1/gsc_data?client_id=eq.${client_id}`, { method: 'DELETE', headers: HEADERS });

  const records = rows.map(row => ({
    client_id,
    query: row.keys[0] || '',
    page: row.keys[1] || '',
    clicks: Math.round(row.clicks || 0),
    impressions: Math.round(row.impressions || 0),
    ctr: parseFloat((row.ctr || 0).toFixed(4)),
    position: parseFloat((row.position || 0).toFixed(1)),
    date_range: `${startDate} to ${endDate}`,
    fetched_at: new Date().toISOString()
  }));

  await fetch(`${SB_URL}/rest/v1/gsc_data`, {
    method: 'POST',
    headers: { ...HEADERS, Prefer: 'return=minimal' },
    body: JSON.stringify(records)
  });

  await fetch(`${SB_URL}/rest/v1/data_sources?client_id=eq.${client_id}&connector_key=eq.google_search_console`, {
    method: 'PATCH',
    headers: { ...HEADERS, Prefer: 'return=minimal' },
    body: JSON.stringify({ last_sync_at: new Date().toISOString() })
  });

  return records.length;
}

// ── OAuth helpers ──────────────────────────────────────────────
async function actionOAuthInit(req, res) {
  const { connector } = req.query;
  if (connector !== 'google_search_console') return res.status(400).json({ error: 'unsupported connector' });
  const params = new URLSearchParams({
    client_id: process.env.GOOGLE_CLIENT_ID,
    redirect_uri: 'https://cmndr-reports.vercel.app/api/connections?action=oauth-callback&connector=google_search_console',
    response_type: 'code',
    scope: 'https://www.googleapis.com/auth/webmasters.readonly',
    access_type: 'offline',
    prompt: 'consent',
    state: req.query.client_id || 'peak-flow',
  });
  return res.redirect(302, `https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}`);
}

async function actionOAuthCallback(req, res) {
  const { code, state: client_id } = req.query;
  if (!code) return res.redirect(302, '/connections?error=no_code');

  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      code,
      client_id: process.env.GOOGLE_CLIENT_ID,
      client_secret: process.env.GOOGLE_CLIENT_SECRET,
      redirect_uri: 'https://cmndr-reports.vercel.app/api/connections?action=oauth-callback&connector=google_search_console',
      grant_type: 'authorization_code',
    })
  });
  const tokens = await tokenRes.json();
  if (!tokens.refresh_token) {
    console.error('[oauth-callback] no refresh_token:', JSON.stringify(tokens));
    return res.redirect(302, '/connections?error=no_refresh_token');
  }

  const encrypted = encrypt(tokens.refresh_token);
  const now = new Date().toISOString();

  await fetch(`${SB_URL}/rest/v1/client_credentials`, {
    method: 'POST',
    headers: UPSERT_H,
    body: JSON.stringify({
      client_id,
      source_system: 'google_search_console',
      credential_type: 'oauth_token',
      encrypted_key: encrypted,
      label: 'GSC Refresh Token',
      scopes: ['https://www.googleapis.com/auth/webmasters.readonly'],
      expires_at: new Date(Date.now() + (tokens.expires_in || 3600) * 1000).toISOString(),
      last_verified_at: now,
      is_active: true,
      updated_at: now
    })
  });

  await fetch(`${SB_URL}/rest/v1/data_sources`, {
    method: 'POST',
    headers: UPSERT_H,
    body: JSON.stringify({
      client_id,
      source_system: 'google_search_console',
      source_label: 'Google Search Console',
      connected: true,
      track: 'live_sync',
      category: 'authority',
      setup_by: 'self_serve',
      connector_key: 'google_search_console',
      last_sync_at: now
    })
  });

  try { await syncGSCData(client_id, tokens.access_token); } catch(e) { console.error('[oauth-callback] sync error:', e.message); }

  return res.redirect(302, '/connections?connected=google_search_console');
}

async function actionGscSync(req, res) {
  const { client_id } = req.body || {};
  if (!client_id) return res.status(400).json({ error: 'client_id required' });

  const rows = await sbGet('client_credentials', `client_id=eq.${encodeURIComponent(client_id)}&source_system=eq.google_search_console&select=encrypted_key&limit=1`);
  if (!rows[0]?.encrypted_key) return res.status(404).json({ error: 'no GSC credentials found' });

  const refreshToken = decrypt(rows[0].encrypted_key);
  const refreshRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      refresh_token: refreshToken,
      client_id: process.env.GOOGLE_CLIENT_ID,
      client_secret: process.env.GOOGLE_CLIENT_SECRET,
      grant_type: 'refresh_token',
    })
  });
  const refreshed = await refreshRes.json();
  if (!refreshed.access_token) return res.status(400).json({ error: 'token refresh failed', detail: refreshed });

  const count = await syncGSCData(client_id, refreshed.access_token);
  return res.json({ success: true, records_synced: count });
}

// ── Action handlers ───────────────────────────────────────────
async function actionList(req, res) {
  const client = req.query.client || req.query.client_id;
  if (!client) return res.status(400).json({ error: 'client required' });

  const [catalogRows, connections] = await Promise.all([
    sbGet('data_sources', `client_id=eq.__catalog__&select=source_id,name,category,icon,description,auth_type`),
    sbGet('data_sources', `client_id=eq.${encodeURIComponent(client)}&select=source_id,connector_key,is_active,connected,last_synced_at,last_sync_at,sync_status`)
  ]);

  const catalog = catalogRows.length > 0 ? catalogRows : DEFAULT_CATALOG;
  const connMap = {};
  for (const c of connections) {
    if (c.source_id) connMap[c.source_id] = c;
    if (c.connector_key) connMap[c.connector_key] = c;
  }

  const sources = catalog.map(item => ({
    ...item,
    connected: !!(connMap[item.id]?.connected || connMap[item.id]?.is_active),
    is_connected: !!(connMap[item.id]?.is_active || connMap[item.id]?.connected),
    connector_key: connMap[item.id]?.connector_key || item.id,
    last_synced_at: connMap[item.id]?.last_synced_at || null,
    last_sync_at: connMap[item.id]?.last_sync_at || connMap[item.id]?.last_synced_at || null,
    sync_status: connMap[item.id]?.sync_status || null,
  }));

  // Also include live connectors (GSC etc) not in catalog
  const liveRows = connections.filter(c => c.connector_key && !catalog.find(cat => cat.id === c.connector_key));
  const allSources = [...sources, ...liveRows.map(c => ({
    id: c.connector_key,
    connector_key: c.connector_key,
    connected: !!(c.connected || c.is_active),
    last_sync_at: c.last_sync_at || c.last_synced_at || null,
  }))];

  res.json({ sources: allSources, data: allSources });
}

async function actionSaveKey(req, res) {
  const { client, source_id, api_key } = req.body;
  if (!client || !source_id || !api_key) return res.status(400).json({ error: 'client, source_id, api_key required' });

  const encrypted = encrypt(api_key);
  const now = new Date().toISOString();

  const credRes = await fetch(`${SB_URL}/rest/v1/client_credentials`, {
    method: 'POST', headers: UPSERT_H,
    body: JSON.stringify({ client_id: client, source_id, encrypted_key: encrypted, updated_at: now })
  });
  if (!credRes.ok) return res.status(500).json({ error: 'credential save failed: ' + await credRes.text() });

  await fetch(`${SB_URL}/rest/v1/data_sources`, {
    method: 'POST', headers: UPSERT_H,
    body: JSON.stringify({ client_id: client, source_id, is_active: true, connected_at: now, sync_status: 'pending' })
  });

  res.json({ ok: true });
}

async function actionTest(req, res) {
  const { client, source_id } = req.body;
  if (!client || !source_id) return res.status(400).json({ error: 'client, source_id required' });

  const rows = await sbGet('client_credentials', `client_id=eq.${encodeURIComponent(client)}&source_id=eq.${encodeURIComponent(source_id)}&select=encrypted_key&limit=1`);
  if (!rows[0]?.encrypted_key) return res.status(404).json({ ok: false, error: 'no credentials found' });

  let ok = true, status = 200, note;
  try {
    const apiKey = decrypt(rows[0].encrypted_key);
    if (source_id === 'stripe') {
      const r = await fetch('https://api.stripe.com/v1/balance', { headers: { Authorization: `Bearer ${apiKey}` } });
      ok = r.ok; status = r.status;
    } else if (source_id === 'mailchimp') {
      const dc = apiKey.split('-').pop();
      const r = await fetch(`https://${dc}.api.mailchimp.com/3.0/ping`, { headers: { Authorization: `apikey ${apiKey}` } });
      ok = r.ok; status = r.status;
    } else if (source_id === 'airtable') {
      const r = await fetch('https://api.airtable.com/v0/meta/whoami', { headers: { Authorization: `Bearer ${apiKey}` } });
      ok = r.ok; status = r.status;
    } else {
      note = 'key saved — full validation on first sync';
    }
  } catch(e) { ok = false; note = e.message; }

  await fetch(`${SB_URL}/rest/v1/data_sources?client_id=eq.${encodeURIComponent(client)}&source_id=eq.${encodeURIComponent(source_id)}`, {
    method: 'PATCH', headers: MIN_H,
    body: JSON.stringify({ sync_status: ok ? 'connected' : 'error', last_tested_at: new Date().toISOString() })
  }).catch(() => {});

  res.json({ ok, status, note });
}

async function actionDisconnect(req, res) {
  const client = req.body.client || req.body.client_id;
  const source_id = req.body.source_id || req.body.connector_key;
  if (!client || !source_id) return res.status(400).json({ error: 'client, source_id required' });

  const isConnectorKey = !req.body.source_id && req.body.connector_key;
  const filter = isConnectorKey
    ? `client_id=eq.${encodeURIComponent(client)}&connector_key=eq.${encodeURIComponent(source_id)}`
    : `client_id=eq.${encodeURIComponent(client)}&source_id=eq.${encodeURIComponent(source_id)}`;

  const r = await fetch(`${SB_URL}/rest/v1/data_sources?${filter}`, {
    method: 'PATCH', headers: MIN_H,
    body: JSON.stringify({ is_active: false, connected: false, sync_status: 'disconnected', disconnected_at: new Date().toISOString() })
  });
  if (!r.ok) return res.status(500).json({ error: await r.text() });
  res.json({ ok: true });
}

function parseCSV(text) {
  const lines = text.trim().split('\n');
  if (!lines.length) return { headers: [], rows: [] };
  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const rows = lines.slice(1, 6).map(line => {
    const vals = line.split(',').map(v => v.trim().replace(/^"|"$/g, ''));
    const row = {};
    headers.forEach((h, i) => { row[h] = vals[i] || ''; });
    return row;
  });
  return { headers, rows };
}

function parseXLSX(buffer) {
  const XLSX = require('xlsx');
  const wb = XLSX.read(buffer, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const data = XLSX.utils.sheet_to_json(ws, { header: 1 });
  if (!data.length) return { headers: [], rows: [] };
  const headers = (data[0] || []).map(h => String(h || '').trim());
  const rows = data.slice(1, 6).map(row => {
    const obj = {};
    headers.forEach((h, i) => { obj[h] = row[i] !== undefined ? String(row[i]) : ''; });
    return obj;
  });
  return { headers, rows };
}

async function actionUpload(req, res) {
  const { client, filename, data, mime_type } = req.body;
  if (!client || !filename || !data) return res.status(400).json({ error: 'client, filename, data required' });

  const buffer = Buffer.from(data, 'base64');
  const isCSV = filename.toLowerCase().endsWith('.csv') || mime_type === 'text/csv';
  const parsed = isCSV ? parseCSV(buffer.toString('utf8')) : parseXLSX(buffer);

  let storage_path = null;
  try {
    const path = `${client}/${Date.now()}-${filename}`;
    const upRes = await fetch(`${SB_URL}/storage/v1/object/client-uploads/${path}`, {
      method: 'POST',
      headers: { apikey: SB_SVC, Authorization: `Bearer ${SB_SVC}`, 'Content-Type': mime_type || 'application/octet-stream' },
      body: buffer
    });
    if (upRes.ok) storage_path = path;
  } catch(e) {}

  await fetch(`${SB_URL}/rest/v1/data_sources`, {
    method: 'POST', headers: UPSERT_H,
    body: JSON.stringify({
      client_id: client, source_id: 'csv_upload', is_active: true,
      connected_at: new Date().toISOString(), sync_status: 'pending_mapping',
      meta: JSON.stringify({ filename, storage_path, row_count: parsed.rows.length })
    })
  }).catch(() => {});

  res.json({ ok: true, filename, storage_path, headers: parsed.headers, preview: parsed.rows, total_columns: parsed.headers.length });
}

function parseAllRows(buffer, filename) {
  const isCSV = filename.toLowerCase().endsWith('.csv');
  if (isCSV) {
    const lines = buffer.toString('utf8').trim().split('\n');
    const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
    return lines.slice(1).filter(l => l.trim()).map(line => {
      const vals = line.split(',').map(v => v.trim().replace(/^"|"$/g, ''));
      const row = {};
      headers.forEach((h, i) => { row[h] = vals[i] || ''; });
      return row;
    });
  }
  const XLSX = require('xlsx');
  const wb = XLSX.read(buffer, { type: 'buffer' });
  return XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);
}

async function actionMapColumns(req, res) {
  const { client, storage_path, filename, mapping, target_table } = req.body;
  if (!client || !mapping || !storage_path) return res.status(400).json({ error: 'client, mapping, storage_path required' });

  const dlRes = await fetch(`${SB_URL}/storage/v1/object/client-uploads/${storage_path}`, {
    headers: { apikey: SB_SVC, Authorization: `Bearer ${SB_SVC}` }
  });
  if (!dlRes.ok) return res.status(500).json({ error: 'Could not download file from storage' });
  const buffer = Buffer.from(await dlRes.arrayBuffer());

  const rows = parseAllRows(buffer, filename || storage_path);
  const mappedRows = rows.map(row => {
    const out = { client_id: client, imported_at: new Date().toISOString() };
    for (const [src, tgt] of Object.entries(mapping)) {
      if (tgt && row[src] !== undefined) out[tgt] = row[src];
    }
    return out;
  }).filter(r => Object.keys(r).length > 2);

  const table = target_table || 'imported_data';
  const BATCH = 500;
  let inserted = 0;
  for (let i = 0; i < mappedRows.length; i += BATCH) {
    const batch = mappedRows.slice(i, i + BATCH);
    const r = await fetch(`${SB_URL}/rest/v1/${table}`, {
      method: 'POST', headers: MIN_H,
      body: JSON.stringify(batch)
    });
    if (!r.ok) return res.status(500).json({ error: `Insert failed at row ${i}: ${await r.text()}` });
    inserted += batch.length;
  }

  await fetch(`${SB_URL}/rest/v1/data_sources?client_id=eq.${encodeURIComponent(client)}&source_id=eq.csv_upload`, {
    method: 'PATCH', headers: MIN_H,
    body: JSON.stringify({ sync_status: 'synced', last_synced_at: new Date().toISOString() })
  }).catch(() => {});

  res.json({ ok: true, inserted, total: mappedRows.length });
}

// ── Main handler ──────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action;
  try {
    if (action === 'list')           return await actionList(req, res);
    if (action === 'save-key')       return await actionSaveKey(req, res);
    if (action === 'test')           return await actionTest(req, res);
    if (action === 'disconnect')     return await actionDisconnect(req, res);
    if (action === 'upload')         return await actionUpload(req, res);
    if (action === 'map-columns')    return await actionMapColumns(req, res);
    if (action === 'oauth-init')     return await actionOAuthInit(req, res);
    if (action === 'oauth-callback') return await actionOAuthCallback(req, res);
    if (action === 'gsc-sync')       return await actionGscSync(req, res);
    res.status(400).json({ error: 'unknown action' });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
};
