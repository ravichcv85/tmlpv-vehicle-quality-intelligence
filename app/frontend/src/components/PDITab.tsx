import { useState } from 'react';
import {
  Box, Grid, TextField, Button, MenuItem, Typography, Card, CardContent,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper,
  Alert, CircularProgress, Chip, ToggleButtonGroup, ToggleButton, Snackbar,
} from '@mui/material';
import AssignmentTurnedInIcon from '@mui/icons-material/AssignmentTurnedIn';
import BuildIcon from '@mui/icons-material/Build';
import WarningIcon from '@mui/icons-material/Warning';
import SecurityIcon from '@mui/icons-material/Security';
import { useApi, postApi } from '../hooks/useApi';
import MetricCard from './MetricCard';

const MODELS = ['Nexon', 'Harrier', 'Safari', 'Tiago', 'Punch', 'Altroz', 'Tigor'];

const CHECKLIST_ITEMS = [
  'Engine Bay', 'Battery & Wiring', 'Brake System', 'Tyre Condition',
  'Exterior Paint', 'Windshield & Glass', 'Interior Trim', 'Infotainment System',
  'AC & Ventilation', 'Seat Belt & Airbags', 'Steering & Suspension', 'Lights & Indicators',
];

interface Dealer {
  dealer_code: string;
  dealer_name: string;
}

interface InspectionResult {
  id: string;
  overall_result: string;
  items_checked: number;
  items_failed: number;
  items_quick_fix: number;
  risk_score: number;
  risk_confidence: number;
}

export default function PDITab() {
  const { data: dealers } = useApi<Dealer[]>('/api/metrics/dealers');
  const { data: recent, refetch: refetchRecent } = useApi<any[]>('/api/inspections/recent');
  const { data: metrics } = useApi<any>('/api/metrics/summary');

  const [form, setForm] = useState({
    vin: '', model: '', variant: '', dealer_code: '', dealer_name: '', inspector_name: '',
  });
  const [checklist, setChecklist] = useState<Record<string, string>>(
    Object.fromEntries(CHECKLIST_ITEMS.map(item => [item, 'Pass']))
  );
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<InspectionResult | null>(null);
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

  const handleChecklistChange = (item: string, value: string | null) => {
    if (value) setChecklist(prev => ({ ...prev, [item]: value }));
  };

  const handleSubmit = async () => {
    if (!form.vin || !form.model || !form.dealer_code || !form.inspector_name) {
      setError('Please fill all required fields');
      return;
    }
    setSubmitting(true);
    setError('');
    setResult(null);
    try {
      const payload = {
        ...form,
        checklist: Object.entries(checklist).map(([name, status]) => ({ name, status })),
      };
      const res = await postApi<InspectionResult>('/api/inspections/submit', payload);
      setResult(res);
      refetchRecent();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const dealerMetrics = metrics?.dealers || {};
  const failCount = Object.values(checklist).filter(v => v === 'Fail').length;
  const qfCount = Object.values(checklist).filter(v => v === 'Quick Fix').length;

  const resultColor = (r: string) => {
    if (r === 'Pass') return 'success';
    if (r === 'Conditional Pass') return 'warning';
    return 'error';
  };

  return (
    <Box>
      {/* Metrics */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Total Inspections"
            value={dealerMetrics.total_inspections || 0}
            icon={<AssignmentTurnedInIcon fontSize="large" />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Total Dealers"
            value={dealerMetrics.total_dealers || 0}
            color="#0d47a1"
            icon={<BuildIcon fontSize="large" />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Avg Fail Rate"
            value={`${dealerMetrics.avg_fail_rate || 0}%`}
            color="#f57c00"
            icon={<WarningIcon fontSize="large" />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Avg Complaint Rate"
            value={`${dealerMetrics.avg_complaint_rate || 0}%`}
            color="#d32f2f"
            icon={<SecurityIcon fontSize="large" />}
          />
        </Grid>
      </Grid>

      {/* Form */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>PDI Inspection Form</Typography>
          <Grid container spacing={2} sx={{ mb: 3 }}>
            <Grid item xs={12} sm={6} md={3}>
              <TextField fullWidth label="VIN *" value={form.vin} onChange={handleChange('vin')} size="small" />
            </Grid>
            <Grid item xs={12} sm={6} md={2}>
              <TextField fullWidth select label="Model *" value={form.model} onChange={handleChange('model')} size="small">
                {MODELS.map(m => <MenuItem key={m} value={m}>{m}</MenuItem>)}
              </TextField>
            </Grid>
            <Grid item xs={12} sm={6} md={2}>
              <TextField fullWidth label="Variant" value={form.variant} onChange={handleChange('variant')} size="small" />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <TextField fullWidth select label="Dealer *" value={form.dealer_code} onChange={handleChange('dealer_code')} size="small">
                {(dealers || []).map(d => (
                  <MenuItem key={d.dealer_code} value={d.dealer_code}>{d.dealer_name} ({d.dealer_code})</MenuItem>
                ))}
              </TextField>
            </Grid>
            <Grid item xs={12} sm={6} md={2}>
              <TextField fullWidth label="Inspector *" value={form.inspector_name} onChange={handleChange('inspector_name')} size="small" />
            </Grid>
          </Grid>

          {/* Checklist */}
          <Typography variant="subtitle1" gutterBottom sx={{ fontWeight: 600 }}>
            Inspection Checklist ({failCount} Failed, {qfCount} Quick Fix)
          </Typography>
          <Grid container spacing={1}>
            {CHECKLIST_ITEMS.map(item => (
              <Grid item xs={12} sm={6} md={4} key={item}>
                <Box display="flex" alignItems="center" justifyContent="space-between"
                  sx={{ p: 1, borderRadius: 1, bgcolor: checklist[item] === 'Fail' ? '#ffebee' : checklist[item] === 'Quick Fix' ? '#fff3e0' : '#e8f5e9' }}>
                  <Typography variant="body2" sx={{ fontWeight: 500, mr: 1 }}>{item}</Typography>
                  <ToggleButtonGroup
                    size="small"
                    value={checklist[item]}
                    exclusive
                    onChange={(_, v) => handleChecklistChange(item, v)}
                  >
                    <ToggleButton value="Pass" sx={{ px: 1, py: 0.3, fontSize: '0.7rem', color: '#2e7d32', '&.Mui-selected': { bgcolor: '#2e7d32', color: '#fff' } }}>Pass</ToggleButton>
                    <ToggleButton value="Fail" sx={{ px: 1, py: 0.3, fontSize: '0.7rem', color: '#d32f2f', '&.Mui-selected': { bgcolor: '#d32f2f', color: '#fff' } }}>Fail</ToggleButton>
                    <ToggleButton value="Quick Fix" sx={{ px: 1, py: 0.3, fontSize: '0.7rem', color: '#f57c00', '&.Mui-selected': { bgcolor: '#f57c00', color: '#fff' } }}>QF</ToggleButton>
                  </ToggleButtonGroup>
                </Box>
              </Grid>
            ))}
          </Grid>

          <Box sx={{ mt: 2 }}>
            <Button
              variant="contained"
              onClick={handleSubmit}
              disabled={submitting}
              startIcon={submitting ? <CircularProgress size={18} /> : <AssignmentTurnedInIcon />}
            >
              {submitting ? 'Submitting...' : 'Submit Inspection'}
            </Button>
          </Box>
        </CardContent>
      </Card>

      {/* Result */}
      {result && (
        <Alert severity={result.overall_result === 'Pass' ? 'success' : result.overall_result === 'Fail' ? 'error' : 'warning'} sx={{ mb: 3 }} onClose={() => setResult(null)}>
          <Typography variant="subtitle2">
            Inspection #{result.id} - Result: {result.overall_result}
          </Typography>
          <Typography variant="body2">
            Items checked: {result.items_checked} | Failed: {result.items_failed} | Quick Fix: {result.items_quick_fix}
          </Typography>
          <Typography variant="body2">
            Risk Score: <strong>{result.risk_score}</strong> (confidence: {(result.risk_confidence * 100).toFixed(0)}%)
          </Typography>
        </Alert>
      )}

      <Snackbar open={!!error} autoHideDuration={6000} onClose={() => setError('')}>
        <Alert severity="error" onClose={() => setError('')}>{error}</Alert>
      </Snackbar>

      {/* Recent Inspections */}
      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>Recent Inspections</Typography>
          <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 400 }}>
            <Table size="small" stickyHeader>
              <TableHead>
                <TableRow>
                  <TableCell>ID</TableCell>
                  <TableCell>VIN</TableCell>
                  <TableCell>Model</TableCell>
                  <TableCell>Dealer</TableCell>
                  <TableCell>Inspector</TableCell>
                  <TableCell>Result</TableCell>
                  <TableCell>Risk Score</TableCell>
                  <TableCell>Cleared</TableCell>
                  <TableCell>Date</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {(recent || []).map((row: any, i: number) => (
                  <TableRow key={i} hover>
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{row.inspection_id}</TableCell>
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{row.vin}</TableCell>
                    <TableCell>{row.model}</TableCell>
                    <TableCell>{row.dealer_name}</TableCell>
                    <TableCell>{row.inspector_name}</TableCell>
                    <TableCell>
                      <Chip label={row.overall_result} color={resultColor(row.overall_result) as any} size="small" />
                    </TableCell>
                    <TableCell>
                      <Typography sx={{ fontWeight: 600, color: Number(row.risk_score) > 50 ? '#d32f2f' : Number(row.risk_score) > 25 ? '#f57c00' : '#2e7d32' }}>
                        {Number(row.risk_score).toFixed(1)}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip label={row.delivery_cleared ? 'Yes' : 'No'} color={row.delivery_cleared ? 'success' : 'error'} size="small" variant="outlined" />
                    </TableCell>
                    <TableCell>{row.inspection_date}</TableCell>
                  </TableRow>
                ))}
                {(!recent || recent.length === 0) && (
                  <TableRow>
                    <TableCell colSpan={9} align="center" sx={{ py: 3 }}>
                      <Typography color="text.secondary">No inspections logged yet</Typography>
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
