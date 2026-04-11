const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;

async function query(table, params) {
  const res = await fetch(`${SB_URL}/rest/v1/${table}?${params}`, {
    headers: {
      apikey: SB_KEY,
      Authorization: `Bearer ${SB_KEY}`,
      'Content-Type': 'application/json',
    },
  });
  return res.json();
}

function fmt$(n) { return '$' + Math.round(n).toLocaleString(); }

function statusColor(s) {
  return s === 'completed' ? '#1D9E75' : s === 'in-progress' ? '#185FA5' : s === 'cancelled' ? '#A32D2D' : '#6b6b67';
}

function prioColor(p) {
  return p === 'high' ? '#993C1D' : p === 'medium' ? '#854F0B' : '#6b6b67';
}

async function getAISummary(jobs, alerts, from, to) {
  const completed = jobs.filter(j => j.status === 'completed').length;
  const inprog = jobs.filter(j => j.status === 'in-progress').length;
  const rev = jobs.filter(j => j.revenue).reduce((a, j) => a + j.revenue, 0);
  const activeAlerts = alerts.filter(a => !a.resolved);
  const topAlert = activeAlerts[0]?.message || 'none';
  try {
    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 200,
        messages: [{ role: 'user', content: `3-sentence operational briefing for Peak Flow Plumbing & HVAC for ${from} to ${to}. Plain text only, no markdown. Data: ${completed} completed, ${inprog} in progress. Revenue: $${Math.round(rev).toLocaleString()}. Top alert: "${topAlert}"` }],
      }),
    });
    const data = await aiRes.json();
    return data.content?.find(b => b.type === 'text')?.text || '';
  } catch(e) { return ''; }
}

function renderEmail(jobs, pipeline, technicians, alerts, summary, from, to, reportUrl) {
  const total = jobs.length;
  const completed = jobs.filter(j => j.status === 'completed').length;
  const inprog = jobs.filter(j => j.status === 'in-progress').length;
  const sched = jobs.filter(j => j.status === 'scheduled').length;
  const rev = jobs.filter(j => j.revenue).reduce((a, j) => a + j.revenue, 0);
  const revJobs = jobs.filter(j => j.revenue);
  const avg = revJobs.length ? rev / revJobs.length : 0;
  const activeAlerts = alerts.filter(a => !a.resolved);
  const fromLabel = new Date(from).toLocaleDateString('en-US', { month: 'long', day: 'numeric' });
  const toLabel = new Date(to).toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });

  const topJobs = [...jobs]
    .sort((a, b) => new Date(b.scheduled_date) - new Date(a.scheduled_date))
    .slice(0, 6);

  const jobRows = topJobs.map(j => `
    <tr>
      <td style="padding:8px 12px;font-size:13px;border-bottom:1px solid #f0f0ee;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${j.title}</td>
      <td style="padding:8px 12px;font-size:13px;border-bottom:1px solid #f0f0ee;color:#6b6b67;">${j.assigned_to}</td>
      <td style="padding:8px 12px;font-size:13px;border-bottom:1px solid #f0f0ee;color:${prioColor(j.priority)};font-weight:500;">${j.priority}</td>
      <td style="padding:8px 12px;font-size:13px;border-bottom:1px solid #f0f0ee;color:${statusColor(j.status)};font-weight:500;">${j.status}</td>
      <td style="padding:8px 12px;font-size:13px;border-bottom:1px solid #f0f0ee;text-align:right;">${j.revenue ? fmt$(j.revenue) : '—'}</td>
    </tr>`).join('');

  const alertRows = activeAlerts.slice(0, 3).map(a => {
    const dot = a.type === 'critical' ? '#E24B4A' : a.type === 'warning' ? '#EF9F27' : '#378ADD';
    return `<tr><td style="padding:8px 12px;font-size:13px;border-bottom:1px solid #f0f0ee;"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${dot};margin-right:8px;vertical-align:middle;"></span>${a.message}</td><td style="padding:8px 12px;font-size:13px;border-bottom:1px solid #f0f0ee;color:#6b6b67;text-transform:capitalize;">${a.type}</td></tr>`;
  }).join('');

  return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Peak Flow — Operational Report</title></head>
<body style="margin:0;padding:0;background:#efefec;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#efefec;padding:32px 16px;">
  <tr><td align="center">
    <table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">

      <!-- Header -->
      <tr><td style="background:#ffffff;border-radius:12px 12px 0 0;padding:28px 32px 24px;border-bottom:1px solid #f0f0ee;">
        <p style="margin:0;font-size:11px;font-weight:500;color:#9c9a92;text-transform:uppercase;letter-spacing:.07em;">Operational report</p>
        <h1 style="margin:6px 0 4px;font-size:22px;font-weight:500;color:#1a1a18;">Peak Flow Plumbing & HVAC</h1>
        <p style="margin:0;font-size:13px;color:#6b6b67;">${fromLabel} — ${toLabel}</p>
      </td></tr>

      <!-- AI Summary -->
      ${summary ? `<tr><td style="background:#f0f7ff;padding:20px 32px;border-bottom:1px solid #f0f0ee;">
        <p style="margin:0 0 6px;font-size:11px;font-weight:500;color:#185FA5;text-transform:uppercase;letter-spacing:.07em;">Claude — operational summary</p>
        <p style="margin:0;font-size:14px;line-height:1.7;color:#1a1a18;">${summary}</p>
      </td></tr>` : ''}

      <!-- KPIs -->
      <tr><td style="background:#ffffff;padding:24px 32px;border-bottom:1px solid #f0f0ee;">
        <p style="margin:0 0 14px;font-size:11px;font-weight:500;color:#9c9a92;text-transform:uppercase;letter-spacing:.07em;">Key metrics</p>
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td style="background:#f5f5f3;border-radius:8px;padding:14px;text-align:center;width:16%;">
              <p style="margin:0;font-size:11px;color:#6b6b67;">Total</p>
              <p style="margin:4px 0 0;font-size:22px;font-weight:500;color:#1a1a18;">${total}</p>
            </td>
            <td width="8"></td>
            <td style="background:#f5f5f3;border-radius:8px;padding:14px;text-align:center;width:16%;">
              <p style="margin:0;font-size:11px;color:#6b6b67;">Completed</p>
              <p style="margin:4px 0 0;font-size:22px;font-weight:500;color:#1D9E75;">${completed}</p>
            </td>
            <td width="8"></td>
            <td style="background:#f5f5f3;border-radius:8px;padding:14px;text-align:center;width:16%;">
              <p style="margin:0;font-size:11px;color:#6b6b67;">In progress</p>
              <p style="margin:4px 0 0;font-size:22px;font-weight:500;color:#185FA5;">${inprog}</p>
            </td>
            <td width="8"></td>
            <td style="background:#f5f5f3;border-radius:8px;padding:14px;text-align:center;width:16%;">
              <p style="margin:0;font-size:11px;color:#6b6b67;">Scheduled</p>
              <p style="margin:4px 0 0;font-size:22px;font-weight:500;color:#1a1a18;">${sched}</p>
            </td>
            <td width="8"></td>
            <td style="background:#f5f5f3;border-radius:8px;padding:14px;text-align:center;width:16%;">
              <p style="margin:0;font-size:11px;color:#6b6b67;">Revenue</p>
              <p style="margin:4px 0 0;font-size:18px;font-weight:500;color:#1a1a18;">${fmt$(rev)}</p>
            </td>
            <td width="8"></td>
            <td style="background:#f5f5f3;border-radius:8px;padding:14px;text-align:center;width:16%;">
              <p style="margin:0;font-size:11px;color:#6b6b67;">Avg value</p>
              <p style="margin:4px 0 0;font-size:18px;font-weight:500;color:#1a1a18;">${fmt$(avg)}</p>
            </td>
          </tr>
        </table>
      </td></tr>

      <!-- Jobs -->
      <tr><td style="background:#ffffff;padding:24px 32px 0;border-bottom:1px solid #f0f0ee;">
        <p style="margin:0 0 14px;font-size:11px;font-weight:500;color:#9c9a92;text-transform:uppercase;letter-spacing:.07em;">Recent jobs</p>
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr style="background:#f5f5f3;">
            <th style="padding:8px 12px;font-size:11px;font-weight:500;color:#6b6b67;text-align:left;">Job</th>
            <th style="padding:8px 12px;font-size:11px;font-weight:500;color:#6b6b67;text-align:left;">Technician</th>
            <th style="padding:8px 12px;font-size:11px;font-weight:500;color:#6b6b67;text-align:left;">Priority</th>
            <th style="padding:8px 12px;font-size:11px;font-weight:500;color:#6b6b67;text-align:left;">Status</th>
            <th style="padding:8px 12px;font-size:11px;font-weight:500;color:#6b6b67;text-align:right;">Value</th>
          </tr>
          ${jobRows}
        </table>
      </td></tr>

      <!-- Alerts -->
      ${activeAlerts.length ? `<tr><td style="background:#ffffff;padding:24px 32px 0;border-bottom:1px solid #f0f0ee;">
        <p style="margin:0 0 14px;font-size:11px;font-weight:500;color:#9c9a92;text-transform:uppercase;letter-spacing:.07em;">Active alerts</p>
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr style="background:#f5f5f3;">
            <th style="padding:8px 12px;font-size:11px;font-weight:500;color:#6b6b67;text-align:left;">Message</th>
            <th style="padding:8px 12px;font-size:11px;font-weight:500;color:#6b6b67;text-align:left;">Type</th>
          </tr>
          ${alertRows}
        </table>
      </td></tr>` : ''}

      <!-- CTA -->
      <tr><td style="background:#ffffff;border-radius:0 0 12px 12px;padding:24px 32px;">
        <a href="${reportUrl}" style="display:inline-block;background:#1a1a18;color:#ffffff;font-size:13px;font-weight:500;padding:12px 24px;border-radius:8px;text-decoration:none;">View live report</a>
        <p style="margin:16px 0 0;font-size:12px;color:#9c9a92;">Generated by CMNDR &middot; Resona &middot; ${new Date().toLocaleDateString('en-US',{month:'long',day:'numeric',year:'numeric'})}</p>
      </td></tr>

    </table>
  </td></tr>
</table>
</body>
</html>`;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const client = req.query.client || 'peak-flow';
  const range = req.query.range || '7d';
  const personalNote = req.query.note ? decodeURIComponent(req.query.note) : '';

  const today = new Date().toISOString().split('T')[0];
  const weekAgo = new Date(); weekAgo.setDate(weekAgo.getDate() - 7);
  const from = req.query.from || weekAgo.toISOString().split('T')[0];
  const to = req.query.to || today;

  const reportUrl = `https://cmndr-reports.vercel.app?client=${client}&range=${range}`;

  const [jobs, pipeline, technicians, alerts] = await Promise.all([
    query('jobs', `client_id=eq.${client}&scheduled_date=gte.${from}&scheduled_date=lte.${to}&order=scheduled_date.desc`),
    query('pipeline_stages', `client_id=eq.${client}&order=week_label.asc`),
    query('technicians', `client_id=eq.${client}&order=utilization_pct.desc`),
    query('alerts', `client_id=eq.${client}&order=severity.desc`),
  ]);

  const summary = await getAISummary(jobs, alerts, from, to);
  let html = renderEmail(jobs, pipeline, technicians, alerts, summary, from, to, reportUrl);

  // Prepend personal note block if provided
  if (personalNote) {
    const noteBlock = `<tr><td style="background:#f5f7ff;border-left:3px solid #185FA5;padding:20px 32px;border-bottom:1px solid #f0f0ee;">
      <p style="margin:0 0 6px;font-size:11px;font-weight:500;color:#185FA5;text-transform:uppercase;letter-spacing:.07em;">Personal note from your advisor</p>
      <p style="margin:0;font-size:14px;line-height:1.7;color:#1a1a18;">${personalNote.replace(/\n/g, '<br>')}</p>
    </td></tr>`;
    html = html.replace('<!-- AI Summary -->', '<!-- Personal Note -->\n      ' + noteBlock + '\n\n      <!-- AI Summary -->');
  }

  res.status(200).json({ html, subject: `Peak Flow — Operational Report (${from} to ${to})`, generatedAt: new Date().toISOString() });
};
