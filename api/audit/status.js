const supabase = require('./_db');

module.exports = async (req, res) => {
  if (req.method !== 'GET') return res.status(405).end();

  const { audit_id } = req.query;
  if (!audit_id) return res.status(400).json({ error: 'audit_id required' });

  const { data, error } = await supabase
    .from('audit_sessions')
    .select('id, status, completed_at')
    .eq('id', audit_id)
    .single();

  if (error || !data) return res.status(404).json({ error: 'Audit not found' });

  return res.status(200).json({
    audit_id: data.id,
    status: data.status,
    completed_at: data.completed_at,
    has_report: data.status === 'complete',
  });
};
