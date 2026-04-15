const supabase = require('./_db');

module.exports = async (req, res) => {
  if (req.method !== 'POST') return res.status(405).end();

  const { audit_id, email, first_name, last_name, phone } = req.body || {};

  if (!audit_id || !email || !first_name || !last_name) {
    return res.status(400).json({ error: 'audit_id, email, first_name, last_name are required' });
  }

  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: 'Invalid email format' });
  }

  const { error } = await supabase
    .from('audit_sessions')
    .update({
      lead_email: email.toLowerCase().trim(),
      lead_first: first_name.trim(),
      lead_last: last_name.trim(),
      lead_phone: phone ? phone.trim() : null,
    })
    .eq('id', audit_id);

  if (error) return res.status(500).json({ error: 'Failed to save lead info' });

  return res.status(200).json({ success: true, audit_id });
};
