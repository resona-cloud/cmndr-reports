const supabase = require('./_db');

module.exports = async (req, res) => {
  if (req.method !== 'GET') return res.status(405).end();

  const { audit_id } = req.query;
  if (!audit_id) return res.status(400).json({ error: 'audit_id required' });

  const { data: session, error: sessErr } = await supabase
    .from('audit_sessions')
    .select('id, business_name, domain, industry, lead_email, status')
    .eq('id', audit_id)
    .single();

  if (sessErr || !session) return res.status(404).json({ error: 'Audit not found' });
  if (!session.lead_email) return res.status(403).json({ error: 'not_unlocked' });

  const { data: scores, error: scoresErr } = await supabase
    .from('panel_scores')
    .select('*')
    .eq('audit_id', audit_id)
    .single();

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
};
