const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const EL_KEY = process.env.ELEVENLABS_API_KEY;
const EL_VOICE = 'EXAVITQu4vr4xnSDxMaL'; // Sarah — Mature, Reassuring, Confident

const CLIENT_META = {
  'peak-flow': { biz: 'Peak Flow Plumbing & HVAC', type: 'plumbing and HVAC service' },
  'demo':      { biz: 'Launchpad Apps', type: 'SaaS application platform' },
};

async function safeQuery(table, params = '') {
  try {
    const r = await fetch(`${SB_URL}/rest/v1/${table}?${params}`, {
      headers: { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' }
    });
    const j = await r.json();
    return Array.isArray(j) ? j : [];
  } catch (e) { return []; }
}

function dateRange(preset) {
  const now = new Date();
  const today = now.toISOString().split('T')[0];
  const days = (n) => { const d = new Date(); d.setDate(d.getDate() - n); return d.toISOString().split('T')[0]; };
  if (preset === '7d')  return { from: days(7),  to: today };
  if (preset === '30d') return { from: days(30), to: today };
  if (preset === 'mtd') return { from: new Date(now.getFullYear(), now.getMonth(), 1).toISOString().split('T')[0], to: today };
  if (preset === 'qtd') { const q = Math.floor(now.getMonth() / 3); return { from: new Date(now.getFullYear(), q * 3, 1).toISOString().split('T')[0], to: today }; }
  return { from: days(7), to: today };
}

async function logUsage(client, inputTokens, outputTokens) {
  const cost = (inputTokens * 0.00000025) + (outputTokens * 0.00000125);
  try {
    await fetch(`${SB_URL}/rest/v1/ai_usage_logs`, {
      method: 'POST',
      headers: { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json', Prefer: 'return=minimal' },
      body: JSON.stringify({
        client_id: client,
        feature: 'audio-briefing',
        model: 'claude-haiku-4-5-20251001',
        input_tokens: inputTokens,
        output_tokens: outputTokens,
        cost_usd: parseFloat(cost.toFixed(6)),
        triggered_by: 'audio-request'
      })
    });
  } catch (e) {}
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const client = req.query.client || 'peak-flow';
  const preset = req.query.range || '7d';
  const from = req.query.from || dateRange(preset).from;
  const to   = req.query.to   || dateRange(preset).to;
  const { biz } = CLIENT_META[client] || { biz: 'the business' };

  // ── Fetch data ────────────────────────────────────────────
  const [jobs, technicians, alerts] = await Promise.all([
    safeQuery('jobs', `client_id=eq.${client}&scheduled_date=gte.${from}&scheduled_date=lte.${to}&order=scheduled_date.desc`),
    safeQuery('technicians', `client_id=eq.${client}&order=utilization_pct.desc`),
    safeQuery('alerts', `client_id=eq.${client}&resolved=eq.false&order=severity.desc`)
  ]);

  const completed = jobs.filter(j => j.status === 'completed').length;
  const inprog    = jobs.filter(j => j.status === 'in-progress').length;
  const weekRev   = jobs.filter(j => j.revenue).reduce((a, j) => a + j.revenue, 0);
  const topAlert  = alerts[0]?.message || 'none';

  // ── Claude summary ────────────────────────────────────────
  let summary = '';
  let inputTokens = 0, outputTokens = 0;
  try {
    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 200,
        messages: [{ role: 'user', content: `3-sentence operational briefing for ${biz} for the period ${from} to ${to}. Plain text only, written naturally for spoken audio — no bullet points, no markdown. Data: ${completed} completed, ${inprog} in progress. Revenue: $${Math.round(weekRev).toLocaleString()}. Technicians: ${technicians.map(t => t.name + ' at ' + t.utilization_pct + '%').join(', ') || 'none'}. Top alert: "${topAlert}"` }]
      })
    });
    const aiData = await aiRes.json();
    summary = aiData.content?.find(b => b.type === 'text')?.text || '';
    inputTokens  = aiData.usage?.input_tokens  || 0;
    outputTokens = aiData.usage?.output_tokens || 0;
  } catch (e) {
    return res.status(500).json({ error: 'Claude generation failed: ' + e.message });
  }

  if (!summary) return res.status(500).json({ error: 'Empty summary from Claude' });

  // ── ElevenLabs TTS ────────────────────────────────────────
  let audio_base64 = '';
  try {
    const elRes = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${EL_VOICE}`, {
      method: 'POST',
      headers: { 'xi-api-key': EL_KEY, 'Content-Type': 'application/json', Accept: 'audio/mpeg' },
      body: JSON.stringify({
        text: summary,
        model_id: 'eleven_flash_v2_5',
        voice_settings: { stability: 0.5, similarity_boost: 0.75 }
      })
    });
    if (!elRes.ok) {
      const errText = await elRes.text();
      return res.status(502).json({ error: 'ElevenLabs error ' + elRes.status + ': ' + errText });
    }
    const audioBuffer = await elRes.arrayBuffer();
    audio_base64 = Buffer.from(audioBuffer).toString('base64');
  } catch (e) {
    return res.status(500).json({ error: 'TTS generation failed: ' + e.message });
  }

  // ── Log usage (fire and forget) ───────────────────────────
  logUsage(client, inputTokens, outputTokens);

  return res.status(200).json({
    audio_base64,
    summary,
    generatedAt: new Date().toISOString()
  });
};
