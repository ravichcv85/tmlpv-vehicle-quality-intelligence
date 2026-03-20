import { useState, useEffect } from 'react';
import {
  Box, Grid, TextField, Button, MenuItem, Typography, Card, CardContent,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper,
  Alert, CircularProgress, Chip, Snackbar,
} from '@mui/material';
import SendIcon from '@mui/icons-material/Send';
import ReportProblemIcon from '@mui/icons-material/ReportProblem';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import TrendingUpIcon from '@mui/icons-material/TrendingUp';
import { useApi, postApi } from '../hooks/useApi';
import MetricCard from './MetricCard';

const MODELS = ['Nexon', 'Harrier', 'Safari', 'Tiago', 'Punch', 'Altroz', 'Tigor'];
const SEVERITIES = ['Low', 'Medium', 'High', 'Critical'];

interface Dealer {
  dealer_code: string;
  dealer_name: string;
}

interface ComplaintResult {
  id: string;
  category: string;
  subcategory: string;
  ai_confidence: number;
  reasoning: string;
  status: string;
}

export default function CRMTab() {
  const { data: dealers } = useApi<Dealer[]>('/api/metrics/dealers');
  const { data: recent, refetch: refetchRecent } = useApi<any[]>('/api/complaints/recent');
  const { data: metrics } = useApi<any>('/api/metrics/summary');

  const [form, setForm] = useState({
    vin: '', model: '', variant: '', customer_name: '',
    dealer_code: '', dealer_name: '', severity: '', description: '',
  });
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<ComplaintResult | null>(null);
  const [error, setError] = useState('');

  const handleChange = (field: string) => (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setForm(prev => {
      const updated = { ...prev, [field]: value };
      if (field === 'dealer_code' && dealers) {
        const dealer = dealers.find(d => d.dealer_code === value);
        if (dealer) updated.dealer_name = dealer.dealer_name;
      }
      return updated;
    });
  };

  const handleSubmit = async () => {
    if (!form.vin || !form.model || !form.dealer_code || !form.severity || !form.description) {
      setError('Please fill all required fields');
      return;
    }
    setSubmitting(true);
    setError('');
    setResult(null);
    try {
      const res = await postApi<ComplaintResult>('/api/complaints/submit', form);
      setResult(res);
      setForm({ vin: '', model: '', variant: '', customer_name: '', dealer_code: '', dealer_name: '', severity: '', description: '' });
      refetchRecent();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const complaintMetrics = metrics?.complaints || {};

  const severityColor = (s: string) => {
    switch (s?.toLowerCase()) {
      case 'critical': return 'error';
      case 'high': return 'warning';
      case 'medium': return 'info';
      default: return 'default';
    }
  };

  return (
    <Box>
      {/* Metrics */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Total Complaints"
            value={complaintMetrics.total_complaints || 0}
            icon={<ReportProblemIcon fontSize="large" />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Open"
            value={complaintMetrics.open_complaints || 0}
            color="#f57c00"
            icon={<ReportProblemIcon fontSize="large" />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Resolved"
            value={complaintMetrics.resolved_complaints || 0}
            color="#2e7d32"
            icon={<CheckCircleIcon fontSize="large" />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Avg AI Confidence"
            value={`${(Number(complaintMetrics.avg_ai_confidence || 0) * 100).toFixed(0)}%`}
            color="#0d47a1"
            icon={<TrendingUpIcon fontSize="large" />}
          />
        </Grid>
      </Grid>

      {/* Form */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>Log New Complaint</Typography>
          <Grid container spacing={2}>
            <Grid item xs={12} sm={6} md={3}>
              <TextField fullWidth label="VIN *" value={form.vin} onChange={handleChange('vin')} size="small" />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <TextField fullWidth select label="Model *" value={form.model} onChange={handleChange('model')} size="small">
                {MODELS.map(m => <MenuItem key={m} value={m}>{m}</MenuItem>)}
              </TextField>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <TextField fullWidth label="Variant" value={form.variant} onChange={handleChange('variant')} size="small" />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <TextField fullWidth label="Customer Name" value={form.customer_name} onChange={handleChange('customer_name')} size="small" />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <TextField fullWidth select label="Dealer *" value={form.dealer_code} onChange={handleChange('dealer_code')} size="small">
                {(dealers || []).map(d => (
                  <MenuItem key={d.dealer_code} value={d.dealer_code}>{d.dealer_name} ({d.dealer_code})</MenuItem>
                ))}
              </TextField>
            </Grid>
            <Grid item xs={12} sm={6} md={2}>
              <TextField fullWidth select label="Severity *" value={form.severity} onChange={handleChange('severity')} size="small">
                {SEVERITIES.map(s => <MenuItem key={s} value={s}>{s}</MenuItem>)}
              </TextField>
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField fullWidth multiline rows={2} label="Description *" value={form.description} onChange={handleChange('description')} size="small" />
            </Grid>
            <Grid item xs={12}>
              <Button
                variant="contained"
                onClick={handleSubmit}
                disabled={submitting}
                startIcon={submitting ? <CircularProgress size={18} /> : <SendIcon />}
              >
                {submitting ? 'Classifying with AI...' : 'Submit Complaint'}
              </Button>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {/* AI Result */}
      {result && (
        <Alert severity="success" sx={{ mb: 3 }} onClose={() => setResult(null)}>
          <Typography variant="subtitle2">
            Complaint #{result.id} logged successfully
          </Typography>
          <Typography variant="body2">
            AI Classification: <strong>{result.category} / {result.subcategory}</strong> (confidence: {(result.ai_confidence * 100).toFixed(0)}%)
          </Typography>
          <Typography variant="body2" color="text.secondary">{result.reasoning}</Typography>
        </Alert>
      )}

      <Snackbar open={!!error} autoHideDuration={6000} onClose={() => setError('')}>
        <Alert severity="error" onClose={() => setError('')}>{error}</Alert>
      </Snackbar>

      {/* Recent Complaints Table */}
      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>Recent Complaints</Typography>
          <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 400 }}>
            <Table size="small" stickyHeader>
              <TableHead>
                <TableRow>
                  <TableCell>ID</TableCell>
                  <TableCell>VIN</TableCell>
                  <TableCell>Model</TableCell>
                  <TableCell>Category</TableCell>
                  <TableCell>Subcategory</TableCell>
                  <TableCell>Severity</TableCell>
                  <TableCell>AI Conf</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Date</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {(recent || []).map((row: any, i: number) => (
                  <TableRow key={i} hover>
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{row.complaint_id}</TableCell>
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{row.vin}</TableCell>
                    <TableCell>{row.model}</TableCell>
                    <TableCell>{row.category}</TableCell>
                    <TableCell>{row.subcategory}</TableCell>
                    <TableCell>
                      <Chip label={row.severity} color={severityColor(row.severity) as any} size="small" />
                    </TableCell>
                    <TableCell>{row.ai_confidence ? `${(Number(row.ai_confidence) * 100).toFixed(0)}%` : '-'}</TableCell>
                    <TableCell>
                      <Chip label={row.status} size="small" variant="outlined" />
                    </TableCell>
                    <TableCell>{row.complaint_date}</TableCell>
                  </TableRow>
                ))}
                {(!recent || recent.length === 0) && (
                  <TableRow>
                    <TableCell colSpan={9} align="center" sx={{ py: 3 }}>
                      <Typography color="text.secondary">No complaints logged yet</Typography>
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>
    </Box>
  );
}
