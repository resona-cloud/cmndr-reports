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

  // ── POST (default) — CMNDR Chat (client-facing) ──────────────
  const { client_id, page, question } = req.body || {};
  if (!client_id || !question) {
    return res.status(400).json({ error: 'client_id and question required' });
  }

  const pageLabel = page || 'overview';
  const start = Date.now();

  // Pull rich context from Supabase based on current page
  let pageContext = '';
  try {
    const endDate = new Date().toISOString().split('T')[0];
    const startDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)
      .toISOString().split('T')[0];

    if (pageLabel === 'operations' || pageLabel === 'overview') {
      const [jobs, techs, alerts, pipeline] = await Promise.all([
        safeQuery('jobs', `client_id=eq.${client_id}&order=scheduled_date.desc&limit=20`),
        safeQuery('technicians', `client_id=eq.${client_id}`),
        safeQuery('alerts', `client_id=eq.${client_id}&resolved=eq.false`),
        safeQuery('pipeline_stages', `client_id=eq.${client_id}&order=week_label.asc`)
      ]);
      const completed = jobs.filter(j => j.status === 'completed').length;
      const inProg = jobs.filter(j => j.status === 'in-progress').length;
      const rev = jobs.filter(j => j.revenue)
        .reduce((a, j) => a + j.revenue, 0);
      pageContext = `
OPERATIONS DATA (last 30 days):
- Total jobs: ${jobs.length} | Completed: ${completed} | In Progress: ${inProg}
- Period revenue: ${Math.round(rev).toLocaleString()}
- Active alerts: ${alerts.length} unresolved
- Technicians: ${techs.map(t => `${t.name} (${t.utilization_pct}% utilization)`).join(', ')}
- Pipeline: ${pipeline.map(p => `${p.stage}: ${p.count} jobs (${p.total_value || 0})`).join(', ')}
`.trim();
    }

    if (pageLabel === 'marketing') {
      const [leads, campaigns, ga4, gsc] = await Promise.all([
        safeQuery('leads', `client_id=eq.${client_id}&order=created_at.desc&limit=50`),
        safeQuery('campaigns', `client_id=eq.${client_id}&active=eq.true`),
        safeQuery('ga4_data', `client_id=eq.${client_id}&order=date.desc&limit=7`),
        safeQuery('gsc_data', `client_id=eq.${client_id}&order=impressions.desc&limit=10`)
      ]);
      const converted = leads.filter(l => l.status === 'converted').length;
      const convRate = leads.length
        ? Math.round(converted / leads.length * 100) : 0;
      const totalSpend = campaigns.reduce((a, c) => a + (c.spend || 0), 0);
      const totalRev = campaigns.reduce((a, c) => a + (c.revenue_attributed || 0), 0);
      const sessions = ga4.reduce((a, r) => a + (r.sessions || 0), 0);
      const topQueries = gsc.slice(0, 5).map(g => g.query).join(', ');
      pageContext = `
MARKETING DATA (last 30 days):
- Leads: ${leads.length} total | ${converted} converted | ${convRate}% conversion rate
- Campaign spend: ${Math.round(totalSpend).toLocaleString()} | Revenue attributed: ${Math.round(totalRev).toLocaleString()}
- GA4 sessions (last 7 days): ${sessions.toLocaleString()}
- Top search queries: ${topQueries || 'no GSC data yet'}
- Active campaigns: ${campaigns.length}
`.trim();
    }

    if (pageLabel === 'finance') {
      const [invoices, snapshots, stripeData, qbData] = await Promise.all([
        safeQuery('invoices', `client_id=eq.${client_id}&order=issued_date.desc&limit=50`),
        safeQuery('revenue_snapshots', `client_id=eq.${client_id}&order=period_start.desc&limit=6`),
        safeQuery('stripe_data', `client_id=eq.${client_id}&order=fetched_at.desc&limit=1`),
        safeQuery('quickbooks_data', `client_id=eq.${client_id}&order=fetched_at.desc&limit=1`)
      ]);
      const paid = invoices.filter(i => i.status === 'paid')
        .reduce((a, i) => a + i.amount, 0);
      const overdue = invoices.filter(i => i.status === 'overdue')
        .reduce((a, i) => a + i.amount, 0);
      const stripe = stripeData[0];
      const qb = qbData[0];
      pageContext = `
FINANCE DATA (last 30 days):
- Invoices: ${invoices.length} total | Paid: ${Math.round(paid).toLocaleString()} | Overdue: ${Math.round(overdue).toLocaleString()}
${stripe ? `- Stripe: ${stripe.total_revenue} revenue | ${stripe.successful_charges} charges | MRR: ${stripe.mrr}` : ''}
${qb ? `- QuickBooks: Revenue ${qb.total_revenue} | Expenses ${qb.total_expenses} | Net ${qb.net_income}` : ''}
- Revenue trend: ${snapshots.map(s => `${s.period_label}: ${Math.round(s.revenue || 0).toLocaleString()}`).join(' | ')}
`.trim();
    }

    if (pageLabel === 'optimization') {
      const [logs, opps, aiUsage] = await Promise.all([
        safeQuery('automation_logs', `client_id=eq.${client_id}&order=ran_at.desc&limit=50`),
        safeQuery('automation_opportunities', `client_id=eq.${client_id}&status=neq.dismissed`),
        safeQuery('ai_usage_logs', `client_id=eq.${client_id}&order=ran_at.desc&limit=20`)
      ]);
      const successRate = logs.length
        ? Math.round(logs.filter(l => l.status === 'success').length / logs.length * 100) : 0;
      const totalCost = aiUsage.reduce((a, l) => a + (parseFloat(l.cost_usd) || 0), 0);
      const potSavings = opps.reduce((a, o) => a + (o.estimated_cost_saving || 0), 0);
      pageContext = `
OPTIMIZATION DATA:
- Automation runs: ${logs.length} | Success rate: ${successRate}%
- Opportunities identified: ${opps.length} | Potential savings: ${Math.round(potSavings).toLocaleString()}/yr
- AI spend: ${totalCost.toFixed(2)} total
`.trim();
    }

    if (pageLabel === 'roadmap') {
      const [roadmap, courses, goals] = await Promise.all([
        safeQuery('roadmaps', `client_id=eq.${client_id}&limit=1`),
        safeQuery('client_courses', `client_id=eq.${client_id}&limit=1`),
        safeQuery('client_goals', `client_id=eq.${client_id}&limit=1`)
      ]);
      const rm = roadmap[0];
      const course = courses[0];
      const goal = goals[0];
      pageContext = `
ROADMAP DATA:
- Goal: ${goal?.stated_goal || 'not set'}
- Current program: ${course?.course_display || 'not enrolled'}
- Roadmap: ${rm ? `Phase ${rm.current_node}/3 active` : 'not configured'}
- Current milestone: ${rm ? JSON.stringify(rm[`milestone_${rm.current_node}`]?.title || '—') : '—'}
`.trim();
    }

  } catch(e) {
    console.error('[ask/context]', e.message);
    pageContext = 'Context unavailable.';
  }

  // Get client goal for system prompt
  let clientGoal = '';
  try {
    const goalRows = await safeQuery('client_goals', `client_id=eq.${client_id}&limit=1`);
    clientGoal = goalRows[0]?.stated_goal || '';
  } catch(e) {}

  let answer = 'I could not generate a response. Please try again.';
  let inputTokens = 0, outputTokens = 0;

  try {
    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 400,
        system: `You are CMNDR, an AI business advisor embedded in a client dashboard.
You are helping a business owner understand their performance data and make better decisions.
${clientGoal ? `Their primary business goal: "${clientGoal}"` : ''}
Current dashboard section: ${pageLabel}

Rules:
- Be direct, specific, and actionable — never generic
- Reference actual numbers from the data when answering
- Keep responses to 2-4 sentences maximum
- If data shows a problem, name it and suggest one concrete next step
- Plain text only — no markdown, no bullet points, no headers
- If you don't have enough data to answer confidently, say so briefly`,
        messages: [{
          role: 'user',
          content: `${question}\n\n${pageContext}`
        }]
      })
    });
    const data = await aiRes.json();
    answer = data.content?.find(b => b.type === 'text')?.text || answer;
    inputTokens = data.usage?.input_tokens || 0;
    outputTokens = data.usage?.output_tokens || 0;
  } catch(e) {
    console.error('[ask/ai]', e.message);
  }

  // Log usage fire-and-forget
  fetch(`${SB_URL}/rest/v1/ai_usage_logs`, {
    method: 'POST',
    headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
    body: JSON.stringify({
      client_id,
      feature: 'cmndr_chat',
      model: 'claude-haiku-4-5-20251001',
      input_tokens: inputTokens,
      output_tokens: outputTokens,
      cost_usd: ((inputTokens * 0.25 + outputTokens * 1.25) / 1_000_000).toFixed(6),
      triggered_by: 'client',
      ran_at: new Date().toISOString()
    })
  }).catch(() => {});

  // Save to cmndr_chat table
  fetch(`${SB_URL}/rest/v1/cmndr_chat`, {
    method: 'POST',
    headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
    body: JSON.stringify({
      client_id,
      page: pageLabel,
      role: 'user',
      content: question,
      created_at: new Date().toISOString()
    })
  }).catch(() => {});

  fetch(`${SB_URL}/rest/v1/cmndr_chat`, {
    method: 'POST',
    headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
    body: JSON.stringify({
      client_id,
      page: pageLabel,
      role: 'assistant',
      content: answer,
      created_at: new Date().toISOString()
    })
  }).catch(() => {});

  const duration = Date.now() - start;
  return res.status(200).json({ answer });
};
