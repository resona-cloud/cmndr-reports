const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SB_ANON = process.env.SUPABASE_ANON_KEY;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const SB_HEADERS = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };

async function safeQuery(table, params) {
  try {
    const r = await fetch(`${SB_URL}/rest/v1/${table}?${params}`, { headers: SB_HEADERS });
    const j = await r.json();
    return Array.isArray(j) ? j : [];
  } catch { return []; }
}

async function verifyResona(req) {
  const token = (req.headers.authorization || '').replace('Bearer ', '');
  if (!token) return null;
  try {
    const userRes = await fetch(`${SB_URL}/auth/v1/user`, {
      headers: { apikey: SB_ANON, Authorization: `Bearer ${token}` }
    });
    if (!userRes.ok) return null;
    const user = await userRes.json();
    const profRes = await fetch(`${SB_URL}/rest/v1/user_profiles?id=eq.${user.id}&select=role,full_name`, { headers: SB_HEADERS });
    const prof = await profRes.json();
    if (!prof[0]?.role?.startsWith('resona_')) return null;
    return { user, profile: prof[0] };
  } catch { return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const type = req.query.type;

  // ── POST ?type=strategy — AI Strategy Console (resona only) ───────────
  if (type === 'strategy') {
    const auth = await verifyResona(req);
    if (!auth) return res.status(403).json({ error: 'Forbidden' });

    const { question, asked_by } = req.body || {};
    if (!question) return res.status(400).json({ error: 'question required' });

    const start = Date.now();

    // Fetch portfolio context in parallel
    const from7 = new Date(Date.now() - 7 * 86400000).toISOString();
    const [healthScores, courses, goals, liveEvents, scenarioHealth, actionHistory] = await Promise.all([
      safeQuery('client_health_scores', 'order=client_id.asc,calculated_at.desc&select=client_id,score,score_label,score_reasoning,operations_score,marketing_score,finance_score,optimization_score,calculated_at'),
      safeQuery('client_courses', 'select=client_id,course_display,level,current_milestone_index,milestones'),
      safeQuery('client_goals', 'select=client_id,stated_goal,set_by'),
      safeQuery('live_events', `order=created_at.desc&limit=50&created_at=gte.${from7}&select=client_id,event_type,event_data,created_at`),
      safeQuery('make_scenario_health', 'order=client_id.asc,scenario_name.asc&select=client_id,scenario_name,status,total_runs,error_count,success_rate,last_run_at'),
      safeQuery('action_history', 'order=decided_at.desc&limit=20&select=client_id,action_title,page,decision,decided_at,outcome')
    ]);

    // Deduplicate health scores (latest per client)
    const latestHealth = {};
    for (const h of healthScores) {
      if (!latestHealth[h.client_id] || h.calculated_at > latestHealth[h.client_id].calculated_at) {
        latestHealth[h.client_id] = h;
      }
    }
    const clientIds = Object.keys(latestHealth);

    const contextStr = `
PORTFOLIO OVERVIEW (${clientIds.length} active clients):

HEALTH SCORES:
${Object.values(latestHealth).map(h =>
  `• ${h.client_id}: ${h.score}/100 (${h.score_label}) — Ops:${h.operations_score} Mktg:${h.marketing_score} Finance:${h.finance_score} Auto:${h.optimization_score}`
).join('\n')}

COURSES & PROGRESS:
${courses.map(c => {
  const done = (c.milestones || []).filter(m => m.status === 'completed').length;
  const total = (c.milestones || []).length;
  return `• ${c.client_id}: ${c.course_display} (Level ${c.level}) — milestone ${c.current_milestone_index + 1}/${total} (${done} completed)`;
}).join('\n')}

CLIENT GOALS:
${goals.map(g => `• ${g.client_id}: "${g.stated_goal}"`).join('\n')}

RECENT EVENTS (last 7 days, ${liveEvents.length} events):
${liveEvents.slice(0, 20).map(e => `• ${e.client_id} — ${e.event_type} at ${e.created_at?.slice(0,10)}`).join('\n')}

AUTOMATION HEALTH:
${scenarioHealth.map(s => `• ${s.client_id}/${s.scenario_name}: ${s.status} (${s.success_rate}% success, ${s.error_count} errors, ${s.total_runs} runs)`).join('\n')}

RECENT ACTIONS:
${actionHistory.map(a => `• ${a.client_id} — ${a.action_title} [${a.page}] → ${a.decision} on ${a.decided_at?.slice(0,10)}`).join('\n')}
`.trim();

    let answer = '';
    let inputTokens = 0, outputTokens = 0;

    try {
      const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({
          model: 'claude-sonnet-4-6',
          max_tokens: 600,
          system: `You are a senior business strategy advisor at Resona, a business coaching firm. You have access to live portfolio data for all clients. Answer questions from Resona staff about portfolio trends, client performance, and strategic priorities. Be specific, cite client IDs when relevant, and give actionable recommendations. Plain text only — no markdown headers or bullet points.`,
          messages: [{ role: 'user', content: `${question}\n\n${contextStr}` }]
        })
      });
      const data = await aiRes.json();
      answer = data.content?.find(b => b.type === 'text')?.text || 'No answer available.';
      inputTokens = data.usage?.input_tokens || 0;
      outputTokens = data.usage?.output_tokens || 0;
    } catch(e) {
      return res.status(500).json({ error: 'AI error: ' + e.message });
    }

    const duration = Date.now() - start;
    const clientsReferenced = clientIds.filter(id => answer.includes(id));

    // Save to strategy_console_history (fire-and-forget)
    fetch(`${SB_URL}/rest/v1/strategy_console_history`, {
      method: 'POST',
      headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
      body: JSON.stringify({
        asked_by: asked_by || auth.profile?.full_name || 'resona_staff',
        question,
        answer,
        clients_referenced: clientsReferenced,
        asked_at: new Date().toISOString()
      })
    }).catch(() => {});

    // Log to ai_usage_logs (fire-and-forget)
    fetch(`${SB_URL}/rest/v1/ai_usage_logs`, {
      method: 'POST',
      headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
      body: JSON.stringify({
        client_id: null, feature: 'strategy-console', prompt_type: 'portfolio_analysis',
        model: 'claude-sonnet-4-6',
        input_tokens: inputTokens, output_tokens: outputTokens,
        total_tokens: inputTokens + outputTokens,
        cost_usd: ((inputTokens * 3 + outputTokens * 15) / 1_000_000).toFixed(6),
        duration_ms: duration, ran_at: new Date().toISOString()
      })
    }).catch(() => {});

    return res.status(200).json({ answer, clients_referenced: clientsReferenced });
  }

  // ── POST (default) — Ask Your Data (client-facing) ────────────────────
  const { client_id, page, question, context_data } = req.body || {};
  if (!client_id || !question) return res.status(400).json({ error: 'client_id and question required' });

  const pageLabel = page || 'general';
  const ctxStr = context_data ? '\n\nPage data context (summarized):\n' + String(context_data).slice(0, 1500) : '';

  const start = Date.now();
  let answer = 'No answer available.';
  let inputTokens = 0, outputTokens = 0;

  try {
    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 350,
        system: `You are a business performance advisor for Resona, analyzing data for client ID "${client_id}". Answer questions about the ${pageLabel} dashboard view. Be concise (2-4 sentences), specific, and actionable. Plain text only — no markdown, no bullet points.`,
        messages: [{ role: 'user', content: question + ctxStr }]
      })
    });
    const data = await aiRes.json();
    answer = data.content?.find(b => b.type === 'text')?.text || answer;
    inputTokens = data.usage?.input_tokens || 0;
    outputTokens = data.usage?.output_tokens || 0;
  } catch(e) {
    return res.status(500).json({ error: 'AI error: ' + e.message });
  }

  // Log usage (fire-and-forget)
  fetch(`${SB_URL}/rest/v1/ai_usage_logs`, {
    method: 'POST',
    headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
    body: JSON.stringify({
      client_id, feature: 'ask_your_data', prompt_type: pageLabel,
      model: 'claude-sonnet-4-6',
      input_tokens: inputTokens, output_tokens: outputTokens,
      total_tokens: inputTokens + outputTokens,
      cost_usd: ((inputTokens * 3 + outputTokens * 15) / 1_000_000).toFixed(6),
      duration_ms: Date.now() - start, ran_at: new Date().toISOString()
    })
  }).catch(() => {});

  return res.status(200).json({ answer });
};
