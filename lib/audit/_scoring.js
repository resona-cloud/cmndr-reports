// ─── Scoring Engine ───────────────────────────────────────────────────────────

function avg(arr) {
  const valid = arr.filter(v => v !== null && v !== undefined && !isNaN(v));
  if (!valid.length) return 3;
  return valid.reduce((a, b) => a + b, 0) / valid.length;
}

function getBand(score) {
  if (score >= 85) return 'Strong';
  if (score >= 70) return 'Stable';
  if (score >= 55) return 'Exposed';
  if (score >= 40) return 'Weak';
  return 'Critical';
}

function getSeverity(score) {
  if (score >= 70) return 'low';
  if (score >= 55) return 'moderate';
  if (score >= 40) return 'high';
  return 'critical';
}

// ─── Firecrawl Signal Parser ──────────────────────────────────────────────────

function parseFirecrawlSignals(markdown) {
  if (!markdown || typeof markdown !== 'string') return defaultFirecrawlSignals();

  const text = markdown.toLowerCase();
  const words = markdown.trim().split(/\s+/);
  const wordCount = words.length;

  // has_clear_headline: specific headline present, not generic
  const hasHeadlineTag = /^#{1,3}\s+.{10,}/m.test(markdown);
  const genericPhrases = /welcome to|home page|we are |about us|click here/i.test(markdown);
  let has_clear_headline = hasHeadlineTag && !genericPhrases ? 4 : hasHeadlineTag ? 2 : 1;

  // has_primary_cta: CTA verbs visible
  const ctaMatches = (markdown.match(/\b(book\s*(now|a\s*call|free)?|get\s*started|start\s*(free|your|a)?|contact\s*us|call\s*(now|us|today)?|schedule|request\s*(a\s*)?(quote|demo|call)|free\s*(quote|consult|trial)|sign\s*up|try\s*(free|now)?|buy\s*now|order\s*now)\b/gi) || []).length;
  const has_primary_cta = ctaMatches >= 3 ? 5 : ctaMatches === 2 ? 4 : ctaMatches === 1 ? 3 : 1;

  // cta_count: competing CTAs penalizes score
  const allCtaCount = (markdown.match(/\b(book|schedule|contact|call|get started|start|sign up|buy|order|request|free)\b/gi) || []).length;
  const cta_count = allCtaCount <= 1 ? 5 : allCtaCount === 2 ? 4 : allCtaCount === 3 ? 3 : 1;

  // has_contact_info: phone, email, address present
  const hasPhone = /(\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}|\bphone\b|\btel\b/i.test(markdown);
  const hasEmail = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/.test(markdown);
  const hasAddress = /\b\d+\s+[\w\s]{2,30}(?:street|st|avenue|ave|road|rd|drive|dr|blvd|boulevard|lane|ln|way|court|ct)\b/i.test(markdown);
  const contactPts = (hasPhone ? 2 : 0) + (hasEmail ? 2 : 0) + (hasAddress ? 1 : 0);
  const has_contact_info = Math.min(5, Math.max(1, contactPts));

  // has_testimonials: review/testimonial content visible
  const testimonialCount = (markdown.match(/\b(testimonial|"[^"]{20,}"|said|says|reviewed?|★|⭐|\d\s*stars?|out of \d|happy\s*customer|client\s*(?:says|review|since))\b/gi) || []).length;
  const has_testimonials = testimonialCount >= 5 ? 5 : testimonialCount >= 3 ? 4 : testimonialCount >= 2 ? 3 : testimonialCount >= 1 ? 2 : 1;

  // has_trust_signals: certifications, years, awards, guarantees
  const trustCount = (markdown.match(/\b(certified|licensed|insured|bonded|award|accredited|bbb|better\s*business|years?\s*(?:in\s*)?(?:business|experience|serving)|since\s*\d{4}|guarantee|warranty|money[- ]back|satisfaction)\b/gi) || []).length;
  const has_trust_signals = trustCount >= 5 ? 5 : trustCount >= 3 ? 4 : trustCount >= 2 ? 3 : trustCount >= 1 ? 2 : 1;

  // service_page_depth: service-related content density
  const serviceCount = (markdown.match(/\b(service|services|what\s*we\s*do|our\s*work|solutions|packages?|pricing|plans?|specializ|offering|expertise)\b/gi) || []).length;
  const service_page_depth = serviceCount >= 10 ? 5 : serviceCount >= 6 ? 4 : serviceCount >= 3 ? 3 : serviceCount >= 1 ? 2 : 1;

  // content_length_signal: word count
  const content_length_signal = wordCount >= 2000 ? 5 : wordCount >= 1000 ? 4 : wordCount >= 600 ? 3 : wordCount >= 300 ? 2 : 1;

  return {
    has_clear_headline,
    has_primary_cta,
    cta_count,
    has_contact_info,
    has_testimonials,
    has_trust_signals,
    service_page_depth,
    content_length_signal,
  };
}

function defaultFirecrawlSignals() {
  return {
    has_clear_headline: 2,
    has_primary_cta: 2,
    cta_count: 3,
    has_contact_info: 2,
    has_testimonials: 2,
    has_trust_signals: 2,
    service_page_depth: 2,
    content_length_signal: 2,
  };
}

// ─── PageSpeed Signal Parser ──────────────────────────────────────────────────

function parsePagespeedSignals(mobileLighthouse, desktopLighthouse) {
  if (!mobileLighthouse || !desktopLighthouse) return defaultPagespeedSignals();

  const mobilePerfScore = mobileLighthouse?.categories?.performance?.score ?? 0.5;
  const mobile_performance = mobilePerfScore * 5;

  const desktopPerfScore = desktopLighthouse?.categories?.performance?.score ?? 0.6;
  const desktop_performance = desktopPerfScore * 5;

  const fcpMs = mobileLighthouse?.audits?.['first-contentful-paint']?.numericValue ?? 3000;
  const fcpSec = fcpMs / 1000;
  const mobile_fcp = fcpSec < 1.8 ? 5 : fcpSec < 3.0 ? 4 : fcpSec < 4.2 ? 3 : fcpSec < 5.5 ? 2 : 1;

  const cls = mobileLighthouse?.audits?.['cumulative-layout-shift']?.numericValue ?? 0.15;
  const mobile_cls = cls < 0.1 ? 5 : cls < 0.25 ? 3 : 1;

  return { mobile_performance, desktop_performance, mobile_fcp, mobile_cls };
}

function defaultPagespeedSignals() {
  return { mobile_performance: 2.5, desktop_performance: 3.0, mobile_fcp: 3, mobile_cls: 3 };
}

// ─── Scoring Engine ───────────────────────────────────────────────────────────

function scoringEngine(questionnaireAnswers, firecrawlSignals, pagespeedSignals) {
  const q = questionnaireAnswers || {};
  const fc = firecrawlSignals || defaultFirecrawlSignals();
  const ps = pagespeedSignals || defaultPagespeedSignals();

  // Map factual inputs to numeric signals (already 1-5 from select values)
  const years = Number(q.years_in_business) || 3;
  const reviews = Number(q.review_count) || 2;
  const rating = Number(q.star_rating) || 2;
  const ads = Number(q.runs_ads) || 2;

  // Revenue Exposure (25%)
  // Signals: offer clarity from site, CTA strength, ads presence
  const revenueRaw = avg([
    fc.has_clear_headline,
    fc.has_primary_cta,
    fc.cta_count,
    ads,               // paid ads = active demand capture
  ]);
  const revenueScore = Math.round(Math.min(100, revenueRaw * 20));

  // Conversion Friction (25%)
  // Signals: mobile performance, speed, contact accessibility
  const conversionRaw = avg([
    ps.mobile_performance,
    ps.mobile_fcp,
    ps.mobile_cls,
    fc.has_contact_info,
  ]);
  const conversionScore = Math.round(Math.min(100, conversionRaw * 20));

  // Trust Surface (20%)
  // Signals: reviews (factual), rating (factual), testimonials on site, trust signals, years
  const trustRaw = avg([
    reviews,             // number of reviews
    rating,              // star rating
    fc.has_testimonials,
    fc.has_trust_signals,
    years,               // years in business = credibility
  ]);
  const trustScore = Math.round(Math.min(100, trustRaw * 20));

  // Market Visibility (15%)
  // Signals: site content depth, reviews presence, ads visibility
  const visibilityRaw = avg([
    fc.service_page_depth,
    fc.content_length_signal,
    reviews,             // review count = market presence signal
    ads,                 // paid ads = visibility signal
  ]);
  const visibilityScore = Math.round(Math.min(100, visibilityRaw * 20));

  // Growth Readiness (15%)
  // Signals: desktop performance, years operational, contact infrastructure
  const growthRaw = avg([
    ps.desktop_performance,
    years,               // longer in business = more operational stability
    fc.has_contact_info,
    fc.service_page_depth,
  ]);
  const growthScore = Math.round(Math.min(100, growthRaw * 20));

  const overallScore = Math.round(
    revenueScore * 0.25 +
    conversionScore * 0.25 +
    trustScore * 0.20 +
    visibilityScore * 0.15 +
    growthScore * 0.15
  );

  const pillars = [
    { key: 'revenue_exposure',    name: 'Revenue Exposure',   score: revenueScore,    weight: 25 },
    { key: 'conversion_friction', name: 'Conversion Friction', score: conversionScore, weight: 25 },
    { key: 'trust_surface',       name: 'Trust Surface',       score: trustScore,      weight: 20 },
    { key: 'market_visibility',   name: 'Market Visibility',   score: visibilityScore, weight: 15 },
    { key: 'growth_readiness',    name: 'Growth Readiness',    score: growthScore,     weight: 15 },
  ].map(p => ({ ...p, band: getBand(p.score), severity: getSeverity(p.score) }));

  const severity_map = {};
  pillars.forEach(p => { severity_map[p.key] = p.severity; });

  return {
    overall_score: overallScore,
    overall_band: getBand(overallScore),
    pillars,
    severity_map,
  };
}

module.exports = { scoringEngine, parseFirecrawlSignals, parsePagespeedSignals };
