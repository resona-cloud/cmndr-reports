const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;

const HEADERS = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, 'Content-Type': 'application/json' };

function defaultMilestones() {
  return [
    { display: 'Assessment & goal alignment', internal: 'completing initial business assessment and setting 90-day goals with the Resona team', status: 'in_progress' },
    { display: 'Process documentation', internal: 'documenting core business processes and identifying key inefficiencies to address', status: 'pending' },
    { display: 'Quick wins implementation', internal: 'implementing 3-5 quick operational improvements for early momentum', status: 'pending' },
    { display: 'Systems & automation setup', internal: 'setting up key tracking systems and automating repetitive tasks', status: 'pending' },
    { display: 'Growth acceleration', internal: 'scaling what is working and building sustainable growth engines', status: 'pending' }
  ];
}

async function fetchCourses() {
  try {
    const r = await fetch(`${SB_URL}/rest/v1/course_library?order=sort_order.asc&select=*`, { headers: HEADERS });
    const rows = await r.json();
    return Array.isArray(rows) ? rows : [];
  } catch { return []; }
}

async function recommendCourse(courses, stated_goal, industry, business_stage) {
  if (!courses.length) return null;
  try {
    const courseList = courses.map(c => `• ${c.course_name}: ${c.description || ''}`).join('\n');
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 250,
        messages: [{
          role: 'user',
          content: `You are a Resona business coach. Based on this client's profile, recommend exactly ONE course from the list below and explain why in 2 sentences.

Client profile:
- Goal: ${stated_goal || 'grow the business'}
- Industry: ${industry || 'not specified'}
- Stage: ${business_stage || 'not specified'}

Available courses:
${courseList}

Reply with JSON only: {"course_name": "...", "reason": "..."}`
        }]
      })
    });
    const d = await r.json();
    const text = d.content?.find(b => b.type === 'text')?.text || '';
    const match = text.match(/\{[\s\S]*\}/);
    if (match) return JSON.parse(match[0]);
    return null;
  } catch { return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // ── GET ?action=courses — fetch course library + AI recommendation ────
  if (req.method === 'GET' && req.query.action === 'courses') {
    const { stated_goal, industry, business_stage } = req.query;
    const courses = await fetchCourses();
    const recommendation = await recommendCourse(courses, stated_goal, industry, business_stage);
    return res.status(200).json({ courses, recommendation });
  }

  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const body = req.body || {};
  const { email, password, full_name, client_id, role, stated_goal, course_id, course_name } = body;

  if (!email || !password || !client_id) {
    return res.status(400).json({ error: 'email, password, and client_id are required' });
  }

  try {
    // 1. Create Supabase auth user (admin API, auto-confirms email)
    const authRes = await fetch(`${SB_URL}/auth/v1/admin/users`, {
      method: 'POST',
      headers: HEADERS,
      body: JSON.stringify({ email, password, email_confirm: true })
    });
    const authData = await authRes.json();
    if (!authRes.ok) throw new Error(authData.message || authData.error_description || 'Failed to create auth user');
    const userId = authData.id;

    // 2. Insert user_profile
    const profRes = await fetch(`${SB_URL}/rest/v1/user_profiles`, {
      method: 'POST',
      headers: { ...HEADERS, Prefer: 'return=minimal' },
      body: JSON.stringify({
        id: userId,
        client_id,
        role: role || 'client_user',
        full_name: full_name || email
      })
    });
    if (!profRes.ok) throw new Error('Failed to create user profile');

    // 3. Insert client_goals
    const goalText = stated_goal || 'Grow the business and improve operational efficiency';
    await fetch(`${SB_URL}/rest/v1/client_goals`, {
      method: 'POST',
      headers: { ...HEADERS, Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: JSON.stringify({ client_id, stated_goal: goalText, set_by: 'resona_onboarding' })
    });

    // 4. Resolve course milestones — prefer course_library if course_id provided
    let courseMilestones = defaultMilestones();
    let resolvedCourseName = course_name || 'Operational Excellence';
    let resolvedCourseInternal = resolvedCourseName.toLowerCase().replace(/\s+/g, '_');

    if (course_id) {
      try {
        const cr = await fetch(`${SB_URL}/rest/v1/course_library?id=eq.${course_id}&select=*`, { headers: HEADERS });
        const cl = await cr.json();
        if (Array.isArray(cl) && cl[0]) {
          resolvedCourseName = cl[0].course_name;
          resolvedCourseInternal = cl[0].course_internal || resolvedCourseName.toLowerCase().replace(/\s+/g, '_');
          if (Array.isArray(cl[0].milestones) && cl[0].milestones.length) {
            courseMilestones = cl[0].milestones;
          }
        }
      } catch {}
    }

    // 5. Insert client_courses
    await fetch(`${SB_URL}/rest/v1/client_courses`, {
      method: 'POST',
      headers: { ...HEADERS, Prefer: 'return=minimal' },
      body: JSON.stringify({
        client_id,
        course_internal: resolvedCourseInternal,
        course_display: resolvedCourseName,
        level: 1,
        current_milestone_index: 0,
        milestones: courseMilestones
      })
    });

    return res.status(200).json({ ok: true, user_id: userId, client_id });
  } catch(e) {
    console.error('[onboard] error:', e.message);
    return res.status(500).json({ error: e.message });
  }
};
