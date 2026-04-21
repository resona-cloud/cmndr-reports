const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;

const CLIENT_META = {
  'peak-flow': { biz: 'Peak Flow Plumbing & HVAC', type: 'plumbing and HVAC service' },
  'demo':       { biz: 'Launchpad Apps', type: 'SaaS application platform' },
};
const DEFAULT_META = { biz: 'the business', type: 'service' };

const SB_HEADERS = {
  apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json'
};

async function query(table, params = '') {
  const res = await fetch(`${SB_URL}/rest/v1/${table}?${params}`, { headers: SB_HEADERS });
  const json = await res.json();
  if (!Array.isArray(json)) { console.error(`[pages] ${table}:`, JSON.stringify(json)); return []; }
  return json;
}

async function safeQuery(table, params = '') {
  try { return await query(table, params); }
  catch (e) { console.error(`[pages] query error ${table}:`, e.message); return []; }
}

async function safeQueryOne(table, params = '') {
  const rows = await safeQuery(table, params);
  return rows[0] || null;
}

async function claude(prompt, maxTokens = 300) {
  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: 'claude-haiku-4-5-20251001', max_tokens: maxTokens, messages: [{ role: 'user', content: prompt }] })
    });
    const d = await res.json();
    return d.content?.find(b => b.type === 'text')?.text || '';
  } catch(e) { return ''; }
}

function courseCtx(course) {
  if (!course) return '';
  const milestones = course.milestones || [];
  const idx = course.current_milestone_index || 0;
  const current = milestones[idx];
  const next = milestones[idx + 1];
  if (!current) return '';
  return `The client is currently focused on: "${current.internal}".${next ? ` After this, they move to: "${next.internal}".` : ''} Orient all guidance around advancing this current focus.`;
}

function shapeCourse(course) {
  if (!course) return null;
  const milestones = course.milestones || [];
  const idx = course.current_milestone_index || 0;
  const current = milestones[idx];
  const next = milestones[idx + 1];
  const completed = milestones.filter(m => m.status === 'completed').length;
  return {
    course_display: course.course_display || '',
    milestone_display: current?.display || '',
    milestone_status: current?.status || 'in_progress',
    completed_milestones: completed,
    total_milestones: milestones.length,
    next_milestone_display: next?.display || ''
  };
}

function goalCtx(goal) {
  if (!goal || !goal.stated_goal) return '';
  return `Client's stated business goal: "${goal.stated_goal}".${goal.goal_context ? ' Context: ' + goal.goal_context : ''} Keep this goal front of mind when giving guidance.`;
}

function notesCtx(notes) {
  const pinned = (notes || []).filter(n => n.pinned);
  if (!pinned.length) return '';
  return `Pinned account notes from Resona team: ${pinned.map(n => `"${n.note}"`).join('; ')}.`;
}

async function advanceMilestone(course) {
  const milestones = [...(course.milestones || [])];
  const idx = course.current_milestone_index || 0;
  if (!milestones[idx] || milestones[idx].status === 'completed') return { advanced: false, course };
  milestones[idx] = { ...milestones[idx], status: 'completed' };
  const nextIdx = idx + 1;
  if (nextIdx < milestones.length) {
    milestones[nextIdx] = { ...milestones[nextIdx], status: 'in_progress' };
  }
  const newIdx = nextIdx < milestones.length ? nextIdx : idx;
  try {
    await fetch(`${SB_URL}/rest/v1/client_courses?id=eq.${course.id}`, {
      method: 'PATCH',
      headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
      body: JSON.stringify({ milestones, current_milestone_index: newIdx, updated_at: new Date().toISOString() })
    });
    return { advanced: true, course: { ...course, milestones, current_milestone_index: newIdx } };
  } catch(e) {
    console.error('[pages] milestone advance error:', e.message);
    return { advanced: false, course };
  }
}

function getMondayOfWeek() {
  const d = new Date();
  const day = d.getUTCDay(); // 0=Sun,1=Mon...
  const diff = (day === 0 ? -6 : 1 - day);
  d.setUTCDate(d.getUTCDate() + diff);
  return d.toISOString().split('T')[0];
}

async function getCachedReport(client, page) {
  try {
    // Demo account: always serve latest is_current snapshot
    // regardless of week date — never generate fresh
    if (client === 'demo') {
      const res = await fetch(
        `${SB_URL}/rest/v1/report_snapshots?client_id=eq.${client}&page=eq.${page}&is_current=eq.true&order=created_at.desc&limit=1`,
        { headers: SB_HEADERS }
      );
      const rows = await res.json();
      if (Array.isArray(rows) && rows.length && rows[0].snapshot_data) {
        return { ...rows[0].snapshot_data, ai_summary: rows[0].ai_summary, from_cache: true, is_demo: true };
      }
      return null;
    }

    // All other clients: match current week
    const weekStart = getMondayOfWeek();
    const res = await fetch(
      `${SB_URL}/rest/v1/report_snapshots?client_id=eq.${client}&page=eq.${page}&is_current=eq.true&period_from=eq.${weekStart}&order=created_at.desc&limit=1`,
      { headers: SB_HEADERS }
    );
    const rows = await res.json();
    if (Array.isArray(rows) && rows.length && rows[0].snapshot_data) {
      return rows[0].snapshot_data;
    }
  } catch (_) {}
  return null;
}

async function clearOldCacheAndSave(client, page, weekStart, payload, summary) {
  // Mark previous is_current rows for this client+page as stale
  fetch(`${SB_URL}/rest/v1/report_snapshots?client_id=eq.${client}&page=eq.${page}&is_current=eq.true`, {
    method: 'PATCH',
    headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
    body: JSON.stringify({ is_current: false })
  }).catch(() => {});

  // Insert new cache row
  fetch(`${SB_URL}/rest/v1/report_snapshots`, {
    method: 'POST',
    headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
    body: JSON.stringify({
      client_id: client,
      period_label: `${page} · week of ${weekStart}`,
      period_from: weekStart,
      period_to: weekStart,
      page,
      is_current: true,
      snapshot_data: payload,
      ai_summary: summary || ''
    })
  }).catch(() => {});
}

function saveSnapshot(client, from, to, page, kpis, summary) {
  fetch(`${SB_URL}/rest/v1/report_snapshots`, {
    method: 'POST',
    headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
    body: JSON.stringify({
      client_id: client,
      period_label: `${page} · ${from} to ${to}`,
      period_from: from,
      period_to: to,
      page,
      snapshot_data: { page, kpis },
      ai_summary: summary || ''
    })
  }).catch(() => {});
}

const HEADERS = SB_HEADERS; // alias used inside handleForecast

async function handleForecast(req, res, client) {
  if (client === 'demo') {
    return res.status(200).json({
      page: 'forecast',
      client: 'demo',
      is_demo: true,
      from_cache: true,
      keywords: ['SaaS platform', 'app marketplace', 'developer tools',
        'no-code builder', 'workflow automation', 'team collaboration',
        'app integration', 'startup tools', 'product analytics',
        'user onboarding'],
      trend_data: [
        { date: '2025-05', values: [{ extracted_value: 48 }] },
        { date: '2025-06', values: [{ extracted_value: 52 }] },
        { date: '2025-07', values: [{ extracted_value: 49 }] },
        { date: '2025-08', values: [{ extracted_value: 55 }] },
        { date: '2025-09', values: [{ extracted_value: 61 }] },
        { date: '2025-10', values: [{ extracted_value: 58 }] },
        { date: '2025-11', values: [{ extracted_value: 63 }] },
        { date: '2025-12', values: [{ extracted_value: 67 }] },
        { date: '2026-01', values: [{ extracted_value: 71 }] },
        { date: '2026-02', values: [{ extracted_value: 69 }] },
        { date: '2026-03', values: [{ extracted_value: 74 }] },
        { date: '2026-04', values: [{ extracted_value: 78 }] }
      ],
      serp_data: [
        { title: 'Bubble — Visual programming platform', domain: 'bubble.io', position: 1 },
        { title: 'Webflow — No-Code Web Design Tool', domain: 'webflow.com', position: 2 },
        { title: 'Glide — Build Apps from Spreadsheets', domain: 'glideapps.com', position: 3 },
        { title: 'Adalo — Build Your Own App', domain: 'adalo.com', position: 4 },
        { title: 'AppGyver — Professional No-Code', domain: 'appgyver.com', position: 5 }
      ],
      market_share: 34,
      wallet_share: 28,
      competitive_cpc: 4.80,
      ai_summary: 'Launchpad Apps operates in a growing SaaS tools market with strong upward trend over the past 12 months — search interest is up 63% year-over-year. Your primary competitors are Bubble, Webflow, and Glide. At an estimated 34% market share in your target keyword set, there is meaningful room to grow. The highest opportunity is in the "workflow automation" and "product analytics" keyword clusters where CPC is lower and conversion intent is higher. Recommend increasing content investment in these two areas before Q3.',
      generatedAt: new Date().toISOString()
    });
  }

  try {
    const kwRes = await fetch(
      `${SB_URL}/rest/v1/client_keywords?client_id=eq.${client}&select=*`,
      { headers: HEADERS }
    );
    const kwRows = await kwRes.json();
    const kwRow = Array.isArray(kwRows) ? kwRows[0] : null;
    const keywords = kwRow?.keywords || [];

    if (!keywords.length) {
      return res.status(200).json({ page: 'forecast', client, keywords: [], no_keywords: true, generatedAt: new Date().toISOString() });
    }

    // Check for fresh cache (within 7 days)
    const cacheRes = await fetch(
      `${SB_URL}/rest/v1/forecast_snapshots?client_id=eq.${client}&order=created_at.desc&limit=1&select=*`,
      { headers: HEADERS }
    );
    const cacheRows = await cacheRes.json();
    const cached = Array.isArray(cacheRows) ? cacheRows[0] : null;
    if (cached && !req.query.force && new Date(cached.expires_at) > new Date()) {
      return res.status(200).json({
        page: 'forecast', client,
        keywords: cached.keywords_used || keywords,
        trend_data: cached.trend_data || [],
        serp_data: cached.serp_data || [],
        market_share: cached.market_share || 0,
        wallet_share: cached.wallet_share || 0,
        competitive_cpc: cached.competitive_cpc || 0,
        ai_summary: cached.ai_summary || '',
        from_cache: true,
        generatedAt: cached.created_at
      });
    }

    const SERPAPI_KEY = process.env.SERPAPI_API_KEY;
    const topKeywords = keywords.slice(0, 3);
    let trendData = [];
    let serpData = [];

    if (SERPAPI_KEY) {
      try {
        const trendsRes = await fetch(
          `https://serpapi.com/search.json?engine=google_trends&q=${encodeURIComponent(topKeywords.join(','))}&date=today+12-m&api_key=${SERPAPI_KEY}`
        );
        const trendsJson = await trendsRes.json();
        trendData = trendsJson.interest_over_time?.timeline_data || [];
      } catch(e) { console.error('[forecast/trends]', e.message); }

      try {
        const serpRes = await fetch(
          `https://serpapi.com/search.json?engine=google&q=${encodeURIComponent(topKeywords[0])}&api_key=${SERPAPI_KEY}&num=10`
        );
        const serpJson = await serpRes.json();
        serpData = (serpJson.organic_results || []).slice(0, 5).map(r => ({
          title: r.title, domain: r.displayed_link, position: r.position
        }));
      } catch(e) { console.error('[forecast/serp]', e.message); }
    }

    const avgInterest = trendData.length
      ? trendData.reduce((a, d) => {
          const vals = (d.values || []).map(v => parseInt(v.extracted_value || 0));
          return a + (vals.reduce((x, y) => x + y, 0) / (vals.length || 1));
        }, 0) / trendData.length
      : 45;
    const market_share = Math.min(100, Math.round(avgInterest * 0.8 + 10));
    const wallet_share = Math.min(100, Math.round((keywords.length * 3) + (avgInterest * 0.3) + 15));
    const competitive_cpc = parseFloat((1.5 + (keywords.length * 0.4) + (Math.random() * 2)).toFixed(2));

    let ai_summary = '';
    try {
      const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({
          model: 'claude-haiku-4-5-20251001', max_tokens: 300,
          messages: [{ role: 'user', content: `You are a market analyst. Write a 3-sentence market summary for a business targeting these keywords: ${topKeywords.join(', ')}. Market share score: ${market_share}/100. Wallet share score: ${wallet_share}/100. Estimated competitive CPC: $${competitive_cpc}. Top competitors: ${serpData.map(s => s.domain).join(', ') || 'unknown'}. Be specific and actionable. Focus on what the scores mean for this market.` }]
        })
      });
      const aiJson = await aiRes.json();
      ai_summary = aiJson.content?.find(b => b.type === 'text')?.text || '';
    } catch(e) { console.error('[forecast/ai]', e.message); }

    fetch(`${SB_URL}/rest/v1/forecast_snapshots`, {
      method: 'POST',
      headers: { ...HEADERS, Prefer: 'return=minimal' },
      body: JSON.stringify({
        client_id: client, keywords_used: keywords, trend_data: trendData, serp_data: serpData,
        market_share, wallet_share, competitive_cpc, ai_summary,
        expires_at: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString()
      })
    }).catch(() => {});

    return res.status(200).json({
      page: 'forecast', client, keywords, trend_data: trendData, serp_data: serpData,
      market_share, wallet_share, competitive_cpc, ai_summary, from_cache: false,
      generatedAt: new Date().toISOString()
    });
  } catch(e) {
    console.error('[forecast]', e.message);
    return res.status(500).json({ error: e.message });
  }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // ── POST: messages, notes, or action history ──────────────
  if (req.method === 'POST') {
    const action = req.query.action;
    const body = req.body || {};

    if (action === 'message') {
      try {
        await fetch(`${SB_URL}/rest/v1/report_messages`, {
          method: 'POST',
          headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
          body: JSON.stringify({
            client_id: body.client_id,
            period_label: body.period_label || '',
            sender_role: body.sender_role || 'client_user',
            sender_name: body.sender_name || 'Unknown',
            message: body.message
          })
        });
        return res.status(200).json({ ok: true });
      } catch(e) { return res.status(500).json({ error: e.message }); }
    }

    if (action === 'note') {
      try {
        await fetch(`${SB_URL}/rest/v1/account_notes`, {
          method: 'POST',
          headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
          body: JSON.stringify({
            client_id: body.client_id,
            author_name: body.author_name || 'Resona',
            author_role: body.author_role || 'resona_user',
            note: body.note,
            note_type: body.note_type || 'general',
            pinned: body.pinned || false
          })
        });
        return res.status(200).json({ ok: true });
      } catch(e) { return res.status(500).json({ error: e.message }); }
    }

    if (action === 'action') {
      try {
        await fetch(`${SB_URL}/rest/v1/action_history`, {
          method: 'POST',
          headers: { ...SB_HEADERS, Prefer: 'return=minimal' },
          body: JSON.stringify({
            client_id: body.client_id,
            action_title: body.action_title,
            action_desc: body.action_desc || '',
            page: body.page || '',
            decision: body.decision,
            decided_by: body.decided_by || '',
            decided_at: new Date().toISOString()
          })
        });
        return res.status(200).json({ ok: true });
      } catch(e) { return res.status(500).json({ error: e.message }); }
    }

    return res.status(400).json({ error: 'Unknown action' });
  }

  // ── GET ───────────────────────────────────────────────────
  const client = req.query.client || 'peak-flow';
  const page = req.query.page || 'operations';
  const from = req.query.from || new Date(Date.now() - 30*24*60*60*1000).toISOString().split('T')[0];
  const to = req.query.to || new Date().toISOString().split('T')[0];
  const { biz: BIZ, type: BIZ_TYPE } = CLIENT_META[client] || DEFAULT_META;

  try {
    // ── CMNDR Chat history ────────────────────────────────────
    if (page === 'cmndr-history') {
      const messages = await safeQuery(
        'cmndr_chat',
        `client_id=eq.${client}&order=created_at.asc&limit=20`
      );
      return res.status(200).json({
        messages: messages || [],
        page: 'cmndr-history',
        client
      });
    }

    // ── History page ─────────────────────────────────────────
    if (page === 'history') {
      const [snapshots, messages] = await Promise.all([
        safeQuery('report_snapshots', `client_id=eq.${client}&order=created_at.desc&limit=20`),
        safeQuery('report_messages', `client_id=eq.${client}&order=created_at.desc&limit=100`)
      ]);
      return res.status(200).json({ snapshots, messages, page, client, from, to, generatedAt: new Date().toISOString() });
    }

    // ── Messages for overview ─────────────────────────────────
    if (page === 'messages') {
      const messages = await safeQuery('report_messages', `client_id=eq.${client}&order=created_at.asc&limit=100`);
      return res.status(200).json({ messages, client, generatedAt: new Date().toISOString() });
    }

    // ── Roadmap ───────────────────────────────────────────────
    if (page === 'roadmap') {
      const roadmapRow = await safeQueryOne('roadmaps', `client_id=eq.${client}`);
      let roadmap = null;
      if (roadmapRow) {
        const toNode = (m, idx) => {
          if (!m) return null;
          const num = idx + 1;
          const currentNode = roadmapRow.current_node || 1;
          const status = num < currentNode ? 'complete' : num === currentNode ? 'active' : 'pending';
          return {
            title: m.title || 'Milestone ' + num,
            description: m.description || '',
            subtitle: m.target_date ? 'Target: ' + new Date(m.target_date).toLocaleDateString('en-US',{month:'short',year:'numeric'}) : '',
            status,
            milestones: m.milestones || [],
          };
        };
        roadmap = {
          current_node: roadmapRow.current_node || 1,
          nodes: [
            toNode(roadmapRow.milestone_1, 0),
            toNode(roadmapRow.milestone_2, 1),
            toNode(roadmapRow.milestone_3, 2),
          ].filter(Boolean),
        };
      }
      return res.status(200).json({ roadmap, page, client, generatedAt: new Date().toISOString() });
    }

    // ── Forecast ──────────────────────────────────────────────
    if (page === 'forecast') {
      return handleForecast(req, res, client);
    }

    // ── Demo short-circuit: serve static snapshots + live data ───
    if (client === 'demo' && page !== 'forecast' && page !== 'cmndr-history') {
      const cached = await getCachedReport(client, page);
      if (cached) {
        const [
          goal, course, health, roadmap, notifications,
          jobs, technicians, pipeline, alerts,
          leads, campaigns, invoices, revenueSnaps,
          logs, opportunities, makeHealth, serviceMetrics
        ] = await Promise.all([
          safeQueryOne('client_goals', `client_id=eq.${client}`),
          safeQueryOne('client_courses', `client_id=eq.${client}`),
          safeQueryOne('client_health_scores', `client_id=eq.${client}&order=calculated_at.desc`),
          safeQueryOne('roadmaps', `client_id=eq.${client}`),
          safeQuery('client_notifications', `client_id=eq.${client}&dismissed=eq.false&order=created_at.desc&limit=5`),
          safeQuery('jobs', `client_id=eq.${client}&order=scheduled_date.desc&limit=25`),
          safeQuery('technicians', `client_id=eq.${client}`),
          safeQuery('pipeline_stages', `client_id=eq.${client}&order=week_label.asc`),
          safeQuery('alerts', `client_id=eq.${client}&resolved=eq.false`),
          safeQuery('leads', `client_id=eq.${client}&order=created_at.desc&limit=30`),
          safeQuery('campaigns', `client_id=eq.${client}&active=eq.true`),
          safeQuery('invoices', `client_id=eq.${client}&order=issued_date.desc&limit=20`),
          safeQuery('revenue_snapshots', `client_id=eq.${client}&order=period_start.desc&limit=6`),
          safeQuery('automation_logs', `client_id=eq.${client}&order=ran_at.desc&limit=50`),
          safeQuery('automation_opportunities', `client_id=eq.${client}&status=neq.dismissed`),
          safeQuery('make_scenario_health', `client_id=eq.${client}`),
          safeQuery('service_metrics', `client_id=eq.${client}&order=week_label.desc&limit=8`)
        ]);

        return res.status(200).json({
          ...cached,
          page,
          client,
          is_demo: true,
          from_cache: true,
          jobs, technicians, pipeline, alerts,
          leads, campaigns,
          invoices,
          snapshots: revenueSnaps,
          logs, opportunities, make_health: makeHealth,
          service_metrics: serviceMetrics,
          goal: goal || null,
          course: shapeCourse(course),
          health: health || null,
          roadmap: roadmap || null,
          notifications: notifications || [],
          generatedAt: new Date().toISOString()
        });
      }
    }

    // ── Common context (every page) ───────────────────────────
    const [courses, goal, notes] = await Promise.all([
      safeQuery('client_courses', `client_id=eq.${client}`),
      safeQueryOne('client_goals', `client_id=eq.${client}`),
      safeQuery('account_notes', `client_id=eq.${client}&order=pinned.desc,created_at.desc`)
    ]);

    let course = courses[0] || null;
    const ctx = courseCtx(course);
    const gCtx = goalCtx(goal);
    const nCtx = notesCtx(notes);
    const context = [ctx, gCtx, nCtx].filter(Boolean).join(' ');

    let data = {};
    let milestone_just_advanced = false;

    // ── Cache check ───────────────────────────────────────────
    const weekStart = getMondayOfWeek();
    const cached = await getCachedReport(client, page);
    if (cached) {
      return res.status(200).json({ ...cached, page, client, from, to, generatedAt: cached.generatedAt || new Date().toISOString(), fromCache: true });
    }

    // ── Operations ────────────────────────────────────────────
    if (page === 'operations') {
      const [jobs, service_metrics, technicians, alerts] = await Promise.all([
        safeQuery('jobs', `client_id=eq.${client}&scheduled_date=gte.${from}&scheduled_date=lte.${to}&order=scheduled_date.desc`),
        safeQuery('service_metrics', `client_id=eq.${client}&week_start=gte.${from}&order=week_start.desc`),
        safeQuery('technicians', `client_id=eq.${client}&order=utilization_pct.desc`),
        safeQuery('alerts', `client_id=eq.${client}&resolved=eq.false&order=severity.desc`)
      ]);

      const completed = jobs.filter(j => j.status === 'completed').length;
      const cancelled = jobs.filter(j => j.status === 'cancelled').length;
      const revJobs = jobs.filter(j => j.revenue);
      const weekRev = revJobs.reduce((a, j) => a + j.revenue, 0);
      const avgRev = revJobs.length ? weekRev / revJobs.length : 0;
      const rate = jobs.length ? Math.round(completed / jobs.length * 100) : 0;
      const metrics = `Jobs: ${jobs.length} total, ${completed} completed (${rate}% rate), ${cancelled} cancelled. Avg job value: $${Math.round(avgRev)}. Revenue: $${Math.round(weekRev)}. Technicians: ${technicians.map(t => t.name + ' ' + t.utilization_pct + '%').join(', ') || 'none'}. Active alerts: ${alerts.length}. Top alert: "${alerts[0]?.message || 'none'}".`;

      // Milestone auto-advance: 80%+ completion rate with at least 4 completed jobs
      if (course && completed >= 4 && rate >= 80) {
        const result = await advanceMilestone(course);
        if (result.advanced) {
          milestone_just_advanced = true;
          course = result.course;
        }
      }

      const [summary, actions, focus, strategyNotes] = await Promise.all([
        claude(`${context} Write a 3-sentence operational analysis for ${BIZ}. Plain text only. Period: ${from} to ${to}. ${metrics} What should they focus on this period?`),
        claude(`${context} Give exactly 3 specific operational actions for ${BIZ} as a JSON array. Each action: {title, description, priority}. Priority must be "high", "medium", or "low". ${metrics} Return only valid JSON array, no markdown.`),
        claude(`You are a senior business consultant guiding a ${BIZ_TYPE} business owner. ${context} Their data (${from} to ${to}): ${metrics} Write 2-3 sentences of specific directional guidance for this week. Use plain language as if speaking directly to the business owner. Never mention courses, milestones, or program names. Focus entirely on practical guidance.`, 200),
        claude(`You are a senior business consultant guiding a ${BIZ_TYPE} business owner. ${context} Their data (${from} to ${to}): ${metrics} Write 2-3 paragraphs covering what the data means in context, what's working, what needs attention, and a clear statement of what comes next. Speak directly to the business owner in practical terms. Never mention courses, milestones, or program names.`, 600)
      ]);

      saveSnapshot(client, from, to, page, { jobs: jobs.length, completed, rate, weekRev }, summary);
      data = { jobs, service_metrics, technicians, alerts, summary, actions, focus, strategyNotes, course: shapeCourse(course), milestone_just_advanced, goal, notes };
      clearOldCacheAndSave(client, page, weekStart, { ...data, generatedAt: new Date().toISOString() }, summary);
    }

    // ── Marketing ─────────────────────────────────────────────
    if (page === 'marketing') {
      const [leads, campaigns] = await Promise.all([
        safeQuery('leads', `client_id=eq.${client}&created_at=gte.${from}&order=created_at.desc`),
        safeQuery('campaigns', `client_id=eq.${client}&order=revenue_attributed.desc`)
      ]);

      const converted = leads.filter(l => l.status === 'converted').length;
      const convRate = leads.length ? Math.round(converted / leads.length * 100) : 0;
      const totalSpend = campaigns.reduce((a, c) => a + (c.spend || 0), 0);
      const totalRevenue = campaigns.reduce((a, c) => a + (c.revenue_attributed || 0), 0);
      const topSource = leads.length
        ? Object.entries(leads.reduce((a, l) => { a[l.source] = (a[l.source] || 0) + 1; return a; }, {})).sort((a, b) => b[1] - a[1])[0][0]
        : 'none';
      const roi = totalSpend ? Math.round((totalRevenue - totalSpend) / totalSpend * 100) : 0;
      const metrics = `Leads: ${leads.length} total, ${converted} converted (${convRate}% rate). Campaign spend: $${Math.round(totalSpend)}, revenue attributed: $${Math.round(totalRevenue)}, blended ROI: ${roi}%. Top lead source: ${topSource}. Active campaigns: ${campaigns.length}.`;

      const [summary, actions, focus, strategyNotes] = await Promise.all([
        claude(`${context} Write a 3-sentence marketing analysis for ${BIZ}. Plain text only. Period: ${from} to ${to}. ${metrics} What should they focus on?`),
        claude(`${context} Give exactly 3 specific marketing actions for ${BIZ} as a JSON array. Each action: {title, description, priority}. Priority must be "high", "medium", or "low". ${metrics} Top campaigns: ${campaigns.slice(0, 3).map(c => c.name + ' ROI:' + (c.spend ? Math.round((c.revenue_attributed - c.spend) / c.spend * 100) : 0) + '%').join(', ')}. Return only valid JSON array, no markdown.`),
        claude(`You are a senior business consultant guiding a ${BIZ_TYPE} business owner. ${context} Their data (${from} to ${to}): ${metrics} Write 2-3 sentences of specific directional guidance for this week. Plain language, directly to the owner. Never mention courses, milestones, or program names.`, 200),
        claude(`You are a senior business consultant guiding a ${BIZ_TYPE} business owner. ${context} Their data (${from} to ${to}): ${metrics} Write 2-3 paragraphs covering what the data means in context, what's working, what needs attention, and what comes next. Speak directly to the owner. Never mention courses, milestones, or program names.`, 600)
      ]);

      saveSnapshot(client, from, to, page, { leads: leads.length, converted, convRate, totalSpend, totalRevenue }, summary);
      data = { leads, campaigns, summary, actions, focus, strategyNotes, course: shapeCourse(course), goal, notes };
      clearOldCacheAndSave(client, page, weekStart, { ...data, generatedAt: new Date().toISOString() }, summary);
    }

    // ── Finance ───────────────────────────────────────────────
    if (page === 'finance') {
      const [invoices, snapshots] = await Promise.all([
        safeQuery('invoices', `client_id=eq.${client}&issued_date=gte.${from}&order=issued_date.desc`),
        safeQuery('revenue_snapshots', `client_id=eq.${client}&order=period_start.desc&limit=6`)
      ]);

      const paid = invoices.filter(i => i.status === 'paid').reduce((a, i) => a + i.amount, 0);
      const outstanding = invoices.filter(i => i.status === 'sent').reduce((a, i) => a + i.amount, 0);
      const overdue = invoices.filter(i => i.status === 'overdue').reduce((a, i) => a + i.amount, 0);
      const overdueCount = invoices.filter(i => i.status === 'overdue').length;
      const total = paid + outstanding + overdue;
      const collRate = total ? Math.round(paid / total * 100) : 0;
      const metrics = `Paid revenue: $${Math.round(paid)}, outstanding: $${Math.round(outstanding)}, overdue: $${Math.round(overdue)} (${overdueCount} invoices). Total invoiced: $${Math.round(total)}. Collection rate: ${collRate}%. Invoice count: ${invoices.length}.`;

      const [summary, actions, focus, strategyNotes] = await Promise.all([
        claude(`${context} Write a 3-sentence financial analysis for ${BIZ}. Plain text only. Period: ${from} to ${to}. ${metrics} What should they focus on?`),
        claude(`${context} Give exactly 3 specific financial actions for ${BIZ} as a JSON array. Each action: {title, description, priority}. Priority must be "high", "medium", or "low". ${metrics} Return only valid JSON array, no markdown.`),
        claude(`You are a senior business consultant guiding a ${BIZ_TYPE} business owner. ${context} Their data (${from} to ${to}): ${metrics} Write 2-3 sentences of specific directional guidance for this week. Plain language, directly to the owner. Never mention courses, milestones, or program names.`, 200),
        claude(`You are a senior business consultant guiding a ${BIZ_TYPE} business owner. ${context} Their data (${from} to ${to}): ${metrics} Write 2-3 paragraphs covering what the data means in context, what's working, what needs attention, and what comes next. Speak directly to the owner. Never mention courses, milestones, or program names.`, 600)
      ]);

      saveSnapshot(client, from, to, page, { paid, outstanding, overdue, collRate }, summary);
      data = { invoices, snapshots, summary, actions, focus, strategyNotes, course: shapeCourse(course), goal, notes, totals: { paid, outstanding, overdue } };
      clearOldCacheAndSave(client, page, weekStart, { ...data, generatedAt: new Date().toISOString() }, summary);
    }

    // ── Optimization ──────────────────────────────────────────
    if (page === 'optimization') {
      const [automation_logs, ai_usage, opportunities] = await Promise.all([
        safeQuery('automation_logs', `client_id=eq.${client}&ran_at=gte.${from}&order=ran_at.desc`),
        safeQuery('ai_usage_logs', `client_id=eq.${client}&ran_at=gte.${from}&order=ran_at.desc`),
        safeQuery('automation_opportunities', `client_id=eq.${client}&order=estimated_cost_saving.desc`)
      ]);

      const totalTokens = ai_usage.reduce((a, l) => a + (l.total_tokens || 0), 0);
      const totalCost = ai_usage.reduce((a, l) => a + (parseFloat(l.cost_usd) || 0), 0);
      const successRate = automation_logs.length ? Math.round(automation_logs.filter(l => l.status === 'success').length / automation_logs.length * 100) : 0;
      const potentialSavings = opportunities.filter(o => o.status !== 'dismissed').reduce((a, o) => a + (o.estimated_cost_saving || 0), 0);
      const metrics = `Automation: ${automation_logs.length} runs, ${successRate}% success rate. AI usage: ${totalTokens.toLocaleString()} tokens, $${totalCost.toFixed(4)} cost. Opportunities: ${opportunities.length} identified with $${Math.round(potentialSavings)} potential annual savings. Top opportunities: ${opportunities.slice(0, 3).map(o => o.title + ' ($' + o.estimated_cost_saving + ' savings)').join(', ') || 'none identified'}.`;

      const [summary, actions, focus, strategyNotes] = await Promise.all([
        claude(`${context} Write a 3-sentence optimization analysis for ${BIZ}. Plain text only. ${metrics} What should they prioritize?`),
        claude(`${context} Give exactly 3 specific automation/AI actions for ${BIZ} as a JSON array. Each action: {title, description, priority}. Priority must be "high", "medium", or "low". ${metrics} Return only valid JSON array, no markdown.`),
        claude(`You are a senior business consultant guiding a ${BIZ_TYPE} business owner. ${context} Their data (${from} to ${to}): ${metrics} Write 2-3 sentences of specific directional guidance for this week around automation and efficiency. Plain language, directly to the owner. Never mention courses, milestones, or program names.`, 200),
        claude(`You are a senior business consultant guiding a ${BIZ_TYPE} business owner. ${context} Their data (${from} to ${to}): ${metrics} Write 2-3 paragraphs covering what the automation data means, what's working, what needs attention, and what they should tackle next. Speak directly to the owner. Never mention courses, milestones, or program names.`, 600)
      ]);

      saveSnapshot(client, from, to, page, { runs: automation_logs.length, successRate, totalTokens, potentialSavings }, summary);
      data = { automation_logs, ai_usage, opportunities, summary, actions, focus, strategyNotes, course: shapeCourse(course), goal, notes, totals: { totalTokens, totalCost, successRate, potentialSavings } };
      clearOldCacheAndSave(client, page, weekStart, { ...data, generatedAt: new Date().toISOString() }, summary);
    }

    res.status(200).json({ ...data, page, client, from, to, generatedAt: new Date().toISOString() });
  } catch(err) {
    console.error('[pages] handler error:', err.message);
    res.status(500).json({ error: err.message });
  }
};
