const supabase = require('./_db');
const { parseFirecrawlSignals, parsePagespeedSignals, scoringEngine } = require('./_scoring');
const { interpretAudit } = require('./_ai');

// waitUntil: run background work after response is sent
let waitUntil;
try {
  ({ waitUntil } = require('@vercel/functions'));
} catch (_) {
  waitUntil = (p) => p.catch(console.error);
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') return res.status(405).end();

  const { business_name, domain: rawDomain, industry, location, goal_focus, questionnaire } = req.body || {};

  if (!business_name || !rawDomain || !industry || !questionnaire) {
    return res.status(400).json({ error: 'Missing required fields: business_name, domain, industry, questionnaire' });
  }

  // Clean and validate domain
  let domain = rawDomain.trim();
  if (!/^https?:\/\//i.test(domain)) domain = 'https://' + domain;
  try { new URL(domain); } catch {
    return res.status(400).json({ error: 'Invalid domain URL' });
  }

  // Create audit session
  const { data, error } = await supabase
    .from('audit_sessions')
    .insert({
      business_name,
      domain,
      industry,
      location: location || null,
      goal_focus: goal_focus || null,
      questionnaire_answers: questionnaire,
      status: 'pending',
      audit_version: 'v1',
      audit_mode: 'lite',
    })
    .select('id')
    .single();

  if (error) {
    console.error('Insert error:', error);
    return res.status(500).json({ error: 'Failed to create audit session' });
  }

  const audit_id = data.id;

  // Kick off pipeline in background (response returns first)
  waitUntil(runPipeline(audit_id, { business_name, domain, industry, questionnaire }));

  return res.status(200).json({ audit_id, status: 'pending' });
};

async function runPipeline(audit_id, { business_name, domain, industry, questionnaire }) {
  try {
    await supabase.from('audit_sessions').update({ status: 'scanning' }).eq('id', audit_id);

    // ── Firecrawl ──────────────────────────────────────────────────────────────
    let firecrawlMarkdown = '';
    try {
      const firecrawlMod = require('@mendable/firecrawl-js');
      const FirecrawlApp = firecrawlMod.default || firecrawlMod;
      const app = new FirecrawlApp({ apiKey: process.env.FIRECRAWL_API_KEY });
      const result = await app.scrapeUrl(domain, { formats: ['markdown'] });
      firecrawlMarkdown = result.markdown || '';

      await supabase.from('source_snapshots').insert({
        audit_id,
        source_type: 'firecrawl',
        raw_data: { url: domain, markdown: firecrawlMarkdown.substring(0, 50000) },
        status: 'success',
      });
    } catch (err) {
      console.error('Firecrawl error:', err.message);
      await supabase.from('source_snapshots').insert({
        audit_id,
        source_type: 'firecrawl',
        raw_data: {},
        status: 'failed',
        error_message: err.message,
      });
    }

    // ── PageSpeed ──────────────────────────────────────────────────────────────
    let mobileLH = null;
    let desktopLH = null;
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
          mobile: mobileLH ? {
            categories: mobileLH.categories,
            audits: {
              'first-contentful-paint': mobileLH.audits?.['first-contentful-paint'],
              'cumulative-layout-shift': mobileLH.audits?.['cumulative-layout-shift'],
            },
          } : null,
          desktop: desktopLH ? { categories: desktopLH.categories } : null,
        },
        status: 'success',
      });
    } catch (err) {
      console.error('PageSpeed error:', err.message);
      await supabase.from('source_snapshots').insert({
        audit_id,
        source_type: 'pagespeed',
        raw_data: {},
        status: 'failed',
        error_message: err.message,
      });
    }

    // ── Scoring ────────────────────────────────────────────────────────────────
    await supabase.from('audit_sessions').update({ status: 'scoring' }).eq('id', audit_id);

    const fcSignals = parseFirecrawlSignals(firecrawlMarkdown);
    const psSignals = parsePagespeedSignals(mobileLH, desktopLH);
    const scores = scoringEngine(questionnaire, fcSignals, psSignals);

    // ── AI Interpretation ──────────────────────────────────────────────────────
    let aiResult = {};
    try {
      aiResult = await interpretAudit({
        business_name,
        industry,
        overall_score: scores.overall_score,
        overall_band: scores.overall_band,
        pillars: scores.pillars,
        severity_map: scores.severity_map,
      });
    } catch (err) {
      console.error('AI error:', err.message);
      aiResult = {
        executive_summary: `${business_name} scored ${scores.overall_score}/100 (${scores.overall_band}).`,
        ai_summary: 'AI analysis unavailable at this time.',
        primary_constraint: 'Unable to determine — AI analysis failed.',
        largest_upside_zone: 'Review pillar scores for opportunities.',
        top_findings: ['Review your Revenue Exposure pillar', 'Review your Conversion Friction pillar', 'Review your Trust Surface pillar'],
        top_opportunity: 'Improve your lowest-scoring pillar first.',
        pillar_drag: {},
        pillar_upside: {},
      };
    }

    // Enrich pillars with drag/upside
    const enrichedPillars = scores.pillars.map(p => ({
      ...p,
      drag: aiResult.pillar_drag?.[p.key] || '',
      upside: aiResult.pillar_upside?.[p.key] || '',
    }));

    // ── Save panel_scores ──────────────────────────────────────────────────────
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

    // ── Complete ───────────────────────────────────────────────────────────────
    await supabase.from('audit_sessions').update({
      status: 'complete',
      completed_at: new Date().toISOString(),
    }).eq('id', audit_id);

  } catch (err) {
    console.error('Pipeline failed:', err);
    await supabase.from('audit_sessions')
      .update({ status: 'failed' })
      .eq('id', audit_id)
      .catch(() => {});
  }
}
