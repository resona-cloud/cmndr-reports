/**
 * Consolidated audit router — handles all /api/audit/* endpoints.
 * Vercel rewrites map /api/audit/:action → /api/audit?_action=:action
 *
 * Actions: start | status | gate | report
 */

const supabase = require('../lib/audit/_db');
const { parseFirecrawlSignals, parsePagespeedSignals, scoringEngine } = require('../lib/audit/_scoring');
const { interpretAudit } = require('../lib/audit/_ai');

let waitUntil;
try { ({ waitUntil } = require('@vercel/functions')); }
catch (_) { waitUntil = (p) => p.catch(console.error); }

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Derive action from rewritten query param or URL path
  let action = req.query._action || req.query.action;
  if (!action) {
    const m = (req.url || '').match(/\/api\/audit\/([^?&/]+)/);
    if (m) action = m[1];
  }

  switch (action) {
    case 'start':  return handleStart(req, res);
    case 'status': return handleStatus(req, res);
    case 'gate':   return handleGate(req, res);
    case 'report': return handleReport(req, res);
    default: return res.status(404).json({ error: 'Unknown audit action' });
  }
};

// ── START ─────────────────────────────────────────────────────────────────────

async function handleStart(req, res) {
  if (req.method !== 'POST') return res.status(405).end();

  const { business_name, domain: rawDomain, industry, location, goal_focus, questionnaire } = req.body || {};
  if (!business_name || !rawDomain || !industry || !questionnaire) {
    return res.status(400).json({ error: 'Missing required fields: business_name, domain, industry, questionnaire' });
  }

  let domain = rawDomain.trim();
  if (!/^https?:\/\//i.test(domain)) domain = 'https://' + domain;
  try { new URL(domain); } catch {
    return res.status(400).json({ error: 'Invalid domain URL' });
  }

  const { data, error } = await supabase
    .from('audit_sessions')
    .insert({ business_name, domain, industry, location: location || null, goal_focus: goal_focus || null, questionnaire_answers: questionnaire, status: 'pending', audit_version: 'v1', audit_mode: 'lite' })
    .select('id').single();

  if (error) { console.error('[audit/start]', error); return res.status(500).json({ error: 'Failed to create audit session' }); }

  const audit_id = data.id;
  waitUntil(runPipeline(audit_id, { business_name, domain, industry, questionnaire }));
  return res.status(200).json({ audit_id, status: 'pending' });
}

// ── STATUS ────────────────────────────────────────────────────────────────────

async function handleStatus(req, res) {
  if (req.method !== 'GET') return res.status(405).end();
  const { audit_id } = req.query;
  if (!audit_id) return res.status(400).json({ error: 'audit_id required' });

  const { data, error } = await supabase.from('audit_sessions').select('id, status, completed_at').eq('id', audit_id).single();
  if (error || !data) return res.status(404).json({ error: 'Audit not found' });

  return res.status(200).json({ audit_id: data.id, status: data.status, completed_at: data.completed_at, has_report: data.status === 'complete' });
}

// ── GATE ──────────────────────────────────────────────────────────────────────

async function handleGate(req, res) {
  if (req.method !== 'POST') return res.status(405).end();
  const { audit_id, email, first_name, last_name, phone } = req.body || {};

  if (!audit_id || !email || !first_name || !last_name) {
    return res.status(400).json({ error: 'audit_id, email, first_name, last_name required' });
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: 'Invalid email format' });
  }

  const { error } = await supabase.from('audit_sessions').update({
    lead_email: email.toLowerCase().trim(),
    lead_first: first_name.trim(),
    lead_last: last_name.trim(),
    lead_phone: phone ? phone.trim() : null,
  }).eq('id', audit_id);

  if (error) return res.status(500).json({ error: 'Failed to save lead info' });
  return res.status(200).json({ success: true, audit_id });
}

// ── REPORT ────────────────────────────────────────────────────────────────────

async function handleReport(req, res) {
  if (req.method !== 'GET') return res.status(405).end();
  const { audit_id } = req.query;
  if (!audit_id) return res.status(400).json({ error: 'audit_id required' });

  const { data: session, error: sessErr } = await supabase.from('audit_sessions').select('id, business_name, domain, industry, lead_email, status').eq('id', audit_id).single();
  if (sessErr || !session) return res.status(404).json({ error: 'Audit not found' });
  if (!session.lead_email) return res.status(403).json({ error: 'not_unlocked' });

  const { data: scores, error: scoresErr } = await supabase.from('panel_scores').select('*').eq('audit_id', audit_id).single();
  if (scoresErr || !scores) return res.status(404).json({ error: 'Report not ready yet' });

  return res.status(200).json({
    audit_id,
    business_name: session.business_name,
    domain: session.domain,
    industry: session.industry,
    overall_score: scores.overall_score,
    overall_band: scores.overall_band,
    executive_summary: scores.executive_summary,
    ai_summary: scores.ai_summary,
    primary_constraint: scores.primary_constraint,
    largest_upside_zone: scores.largest_upside_zone,
    top_findings: scores.top_findings || [],
    top_opportunity: scores.top_opportunity,
    pillars: scores.pillar_scores_json || [],
    severity_map: scores.severity_map || {},
  });
}

// ── PIPELINE (background) ─────────────────────────────────────────────────────

async function runPipeline(audit_id, { business_name, domain, industry, questionnaire }) {
  try {
    await supabase.from('audit_sessions').update({ status: 'scanning' }).eq('id', audit_id);

    let firecrawlMarkdown = '';
    try {
      const firecrawlMod = require('@mendable/firecrawl-js');
      const FirecrawlApp = firecrawlMod.default || firecrawlMod;
      const app = new FirecrawlApp({ apiKey: process.env.FIRECRAWL_API_KEY });
      const result = await app.scrapeUrl(domain, { formats: ['markdown'] });
      firecrawlMarkdown = result.markdown || '';
      await supabase.from('source_snapshots').insert({ audit_id, source_type: 'firecrawl', raw_data: { url: domain, markdown: firecrawlMarkdown.substring(0, 50000) }, status: 'success' });
    } catch (err) {
      console.error('[audit/firecrawl]', err.message);
      await supabase.from('source_snapshots').insert({ audit_id, source_type: 'firecrawl', raw_data: {}, status: 'failed', error_message: err.message });
    }

    let mobileLH = null, desktopLH = null;
    try {
      const [mRes, dRes] = await Promise.all([
        fetch(`https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url=${encodeURIComponent(domain)}&strategy=mobile`),
        fetch(`https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url=${encodeURIComponent(domain)}&strategy=desktop`),
      ]);
      const mData = await mRes.json();
      const dData = await dRes.json();
      mobileLH = mData?.lighthouseResult || null;
      desktopLH = dData?.lighthouseResult || null;
      await supabase.from('source_snapshots').insert({
        audit_id,
        source_type: 'pagespeed',
        raw_data: {
          mobile: mobileLH ? { categories: mobileLH.categories, audits: { 'first-contentful-paint': mobileLH.audits?.['first-contentful-paint'], 'cumulative-layout-shift': mobileLH.audits?.['cumulative-layout-shift'] } } : null,
          desktop: desktopLH ? { categories: desktopLH.categories } : null,
        },
        status: 'success',
      });
    } catch (err) {
      console.error('[audit/pagespeed]', err.message);
      await supabase.from('source_snapshots').insert({ audit_id, source_type: 'pagespeed', raw_data: {}, status: 'failed', error_message: err.message });
    }

    await supabase.from('audit_sessions').update({ status: 'scoring' }).eq('id', audit_id);

    const fcSignals = parseFirecrawlSignals(firecrawlMarkdown);
    const psSignals = parsePagespeedSignals(mobileLH, desktopLH);
    const scores = scoringEngine(questionnaire, fcSignals, psSignals);

    let aiResult = {};
    try {
      aiResult = await interpretAudit({ business_name, industry, overall_score: scores.overall_score, overall_band: scores.overall_band, pillars: scores.pillars, severity_map: scores.severity_map });
    } catch (err) {
      console.error('[audit/ai]', err.message);
      aiResult = {
        executive_summary: `${business_name} scored ${scores.overall_score}/100 (${scores.overall_band}).`,
        ai_summary: 'AI analysis unavailable at this time.',
        primary_constraint: 'See pillar scores for primary constraint.',
        largest_upside_zone: 'Review your lowest-scoring pillar.',
        top_findings: ['Review your Revenue Exposure score', 'Review your Conversion Friction score', 'Review your Trust Surface score'],
        top_opportunity: 'Improve the lowest-scoring pillar first for maximum impact.',
        pillar_drag: {}, pillar_upside: {},
      };
    }

    const enrichedPillars = scores.pillars.map(p => ({ ...p, drag: aiResult.pillar_drag?.[p.key] || '', upside: aiResult.pillar_upside?.[p.key] || '' }));

    await supabase.from('panel_scores').insert({
      audit_id,
      overall_score: scores.overall_score,
      overall_band: scores.overall_band,
      primary_constraint: aiResult.primary_constraint || '',
      largest_upside_zone: aiResult.largest_upside_zone || '',
      executive_summary: aiResult.executive_summary || '',
      top_findings: aiResult.top_findings || [],
      top_opportunity: aiResult.top_opportunity || '',
      ai_summary: aiResult.ai_summary || '',
      pillar_scores_json: enrichedPillars,
      severity_map: scores.severity_map,
    });

    await supabase.from('audit_sessions').update({ status: 'complete', completed_at: new Date().toISOString() }).eq('id', audit_id);

  } catch (err) {
    console.error('[audit/pipeline]', err);
    await supabase.from('audit_sessions').update({ status: 'failed' }).eq('id', audit_id).catch(() => {});
  }
}
