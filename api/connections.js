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

// ── Action handlers ───────────────────────────────────────────
async function actionList(req, res) {
  const { client } = req.query;
  if (!client) return res.status(400).json({ error: 'client required' });

  const [catalogRows, connections] = await Promise.all([
    sbGet('data_sources', `client_id=eq.__catalog__&select=source_id,name,category,icon,description,auth_type`),
    sbGet('data_sources', `client_id=eq.${encodeURIComponent(client)}&select=source_id,is_active,last_synced_at,sync_status`)
  ]);

  const catalog = catalogRows.length > 0 ? catalogRows : DEFAULT_CATALOG;
  const connMap = {};
  for (const c of connections) connMap[c.source_id] = c;

  const sources = catalog.map(item => ({
    ...item,
    is_connected: !!(connMap[item.id]?.is_active),
    last_synced_at: connMap[item.id]?.last_synced_at || null,
    sync_status: connMap[item.id]?.sync_status || null,
  }));

  res.json({ sources });
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
  const { client, source_id } = req.body;
  if (!client || !source_id) return res.status(400).json({ error: 'client, source_id required' });

  const r = await fetch(`${SB_URL}/rest/v1/data_sources?client_id=eq.${encodeURIComponent(client)}&source_id=eq.${encodeURIComponent(source_id)}`, {
    method: 'PATCH', headers: MIN_H,
    body: JSON.stringify({ is_active: false, sync_status: 'disconnected', disconnected_at: new Date().toISOString() })
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
    if (action === 'list')         return await actionList(req, res);
    if (action === 'save-key')     return await actionSaveKey(req, res);
    if (action === 'test')         return await actionTest(req, res);
    if (action === 'disconnect')   return await actionDisconnect(req, res);
    if (action === 'upload')       return await actionUpload(req, res);
    if (action === 'map-columns')  return await actionMapColumns(req, res);
    res.status(400).json({ error: 'unknown action' });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
};
