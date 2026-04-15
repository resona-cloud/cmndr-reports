const Anthropic = require('@anthropic-ai/sdk');

const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

async function interpretAudit({ business_name, industry, overall_score, overall_band, pillars, severity_map }) {
  const pillarText = (pillars || [])
    .map(p => `${p.name}: ${p.score}/100 (${p.band}) - severity: ${p.severity}`)
    .join('\n');

  const prompt = `You are a business analyst generating an audit report for a business.
Be specific, direct, and actionable. Never use generic filler language.

Business: ${business_name}
Industry: ${industry}
Overall Score: ${overall_score}/100 (${overall_band})

Pillar Scores:
${pillarText}

Generate the following as a JSON object only, no other text:
{
  "executive_summary": "2-3 sentences. State the primary exposure pattern, its likely business impact, and the single most important area to address. Be specific to this business.",
  "ai_summary": "4-5 sentences expanding on the executive summary with specific observations per the weakest pillars.",
  "primary_constraint": "One sentence naming the single biggest drag on growth.",
  "largest_upside_zone": "One sentence naming where improvement would create the fastest lift.",
  "top_findings": [
    "Finding 1: observation + likely implication + why it matters",
    "Finding 2: observation + likely implication + why it matters",
    "Finding 3: observation + likely implication + why it matters"
  ],
  "top_opportunity": "One specific, actionable improvement that would create the most immediate impact.",
  "pillar_drag": {
    "revenue_exposure": "one sentence drag statement",
    "conversion_friction": "one sentence drag statement",
    "trust_surface": "one sentence drag statement",
    "market_visibility": "one sentence drag statement",
    "growth_readiness": "one sentence drag statement"
  },
  "pillar_upside": {
    "revenue_exposure": "one sentence upside statement",
    "conversion_friction": "one sentence upside statement",
    "trust_surface": "one sentence upside statement",
    "market_visibility": "one sentence upside statement",
    "growth_readiness": "one sentence upside statement"
  }
}`;

  const response = await client.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 1500,
    messages: [{ role: 'user', content: prompt }],
  });

  const text = response.content[0].text.trim();
  const jsonMatch = text.match(/\{[\s\S]*\}/);
  if (!jsonMatch) throw new Error('AI response did not contain valid JSON');

  return JSON.parse(jsonMatch[0]);
}

module.exports = { interpretAudit };
