import { useState, useMemo } from 'react';
import {
  Box, Grid, TextField, Button, MenuItem, Typography, Card, CardContent,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper,
  Alert, CircularProgress, Chip,
} from '@mui/material';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import AnalyticsIcon from '@mui/icons-material/Analytics';
import GppBadIcon from '@mui/icons-material/GppBad';
import GppGoodIcon from '@mui/icons-material/GppGood';
import { useApi, postApi } from '../hooks/useApi';
import MetricCard from './MetricCard';

interface CategoryOption {
  category: string;
  subcategory: string;
}

interface GapResult {
  gap_data: any[];
  recommendation: string;
}

export default function ChecklistAgentTab() {
  const { data: categories } = useApi<CategoryOption[]>('/api/checklist-agent/categories');
  const { data: fullTable } = useApi<any[]>('/api/checklist-agent/full-table');
  const { data: metrics } = useApi<any>('/api/metrics/summary');

  const [category, setCategory] = useState('');
  const [subcategory, setSubcategory] = useState('');
  const [analyzing, setAnalyzing] = useState(false);
  const [result, setResult] = useState<GapResult | null>(null);
  const [error, setError] = useState('');

  const uniqueCategories = useMemo(() => {
    if (!categories) return [];
    return [...new Set(categories.map(c => c.category))].sort();
  }, [categories]);

  const subcategories = useMemo(() => {
    if (!categories || !category) return [];
    return categories.filter(c => c.category === category).map(c => c.subcategory).sort();
  }, [categories, category]);

  const handleAnalyze = async () => {
    if (!category) return;
    setAnalyzing(true);
    setError('');
    setResult(null);
    try {
      const res = await postApi<GapResult>('/api/checklist-agent/analyze', { category, subcategory });
      setResult(res);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setAnalyzing(false);
    }
  };

  const gapMetrics = metrics?.gaps || {};

  const gapColor = (gap: string) => {
    if (gap?.includes('Critical')) return 'error';
    if (gap?.includes('Major') || gap?.includes('Significant')) return 'warning';
    return 'success';
  };

  return (
    <Box>
      {/* Metrics */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={4}>
          <MetricCard
            title="Total Gap Items"
            value={gapMetrics.total_gaps || 0}
            icon={<AnalyticsIcon fontSize="large" />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={4}>
          <MetricCard
            title="Critical Gaps"
            value={gapMetrics.critical_gaps || 0}
            color="#d32f2f"
            icon={<GppBadIcon fontSize="large" />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={4}>
          <MetricCard
            title="Avg PDI Catch Rate"
            value={`${gapMetrics.avg_catch_rate || 0}%`}
            color="#2e7d32"
            icon={<GppGoodIcon fontSize="large" />}
          />
        </Grid>
      </Grid>

      {/* Analysis Form */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            <SmartToyIcon sx={{ mr: 1, verticalAlign: 'middle' }} />
            AI Checklist Gap Analyzer
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Select a complaint category to analyze gaps in the PDI checklist. The AI agent will recommend new inspection items to catch these complaints earlier.
          </Typography>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} sm={4}>
              <TextField
                fullWidth select label="Category *" value={category} size="small"
                onChange={e => { setCategory(e.target.value); setSubcategory(''); }}
              >
                {uniqueCategories.map(c => <MenuItem key={c} value={c}>{c}</MenuItem>)}
              </TextField>
            </Grid>
            <Grid item xs={12} sm={4}>
              <TextField
                fullWidth select label="Subcategory" value={subcategory} size="small"
                onChange={e => setSubcategory(e.target.value)}
                disabled={!category}
              >
                <MenuItem value="">All</MenuItem>
                {subcategories.map(s => <MenuItem key={s} value={s}>{s}</MenuItem>)}
              </TextField>
            </Grid>
            <Grid item xs={12} sm={4}>
              <Button
                variant="contained"
                onClick={handleAnalyze}
                disabled={analyzing || !category}
                startIcon={analyzing ? <CircularProgress size={18} /> : <SmartToyIcon />}
                fullWidth
              >
                {analyzing ? 'AI Analyzing...' : 'Analyze Gap'}
              </Button>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {error && <Alert severity="error" sx={{ mb: 3 }}>{error}</Alert>}

      {/* AI Recommendation */}
      {result && (
        <Card sx={{ mb: 3, border: '2px solid #1a237e' }}>
          <CardContent>
            <Typography variant="h6" gutterBottom sx={{ color: '#1a237e' }}>
              <SmartToyIcon sx={{ mr: 1, verticalAlign: 'middle' }} />
              AI Recommendation
            </Typography>
            <Box sx={{ whiteSpace: 'pre-wrap', fontFamily: '"Inter", sans-serif', lineHeight: 1.7, fontSize: '0.9rem' }}>
              {result.recommendation}
            </Box>

            {result.gap_data.length > 0 && (
              <Box sx={{ mt: 3 }}>
                <Typography variant="subtitle1" gutterBottom sx={{ fontWeight: 600 }}>Gap Data</Typography>
                <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 300 }}>
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        <TableCell>Category</TableCell>
                        <TableCell>Subcategory</TableCell>
                        <TableCell>Complaints</TableCell>
                        <TableCell>Mapped Item</TableCell>
                        <TableCell>Catch Rate</TableCell>
                        <TableCell>Gap</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {result.gap_data.map((row, i) => (
                        <TableRow key={i} hover>
                          <TableCell>{row.category}</TableCell>
                          <TableCell>{row.subcategory}</TableCell>
                          <TableCell>{row.complaint_count}</TableCell>
                          <TableCell>{row.mapped_inspection_item || '-'}</TableCell>
                          <TableCell>{row.pdi_catch_rate_pct}%</TableCell>
                          <TableCell>
                            <Chip label={row.gap_classification} color={gapColor(row.gap_classification) as any} size="small" />
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Box>
            )}
          </CardContent>
        </Card>
      )}

      {/* Full Gap Table */}
      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>Full Checklist Gap Analysis</Typography>
          <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 400 }}>
            <Table size="small" stickyHeader>
              <TableHead>
                <TableRow>
                  <TableCell>Category</TableCell>
                  <TableCell>Subcategory</TableCell>
                  <TableCell>Complaints</TableCell>
                  <TableCell>Affected VINs</TableCell>
                  <TableCell>AI Confidence</TableCell>
                  <TableCell>Mapped Item</TableCell>
                  <TableCell>PDI Catch Rate</TableCell>
                  <TableCell>Gap</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {(fullTable || []).map((row: any, i: number) => (
                  <TableRow key={i} hover>
                    <TableCell>{row.category}</TableCell>
                    <TableCell>{row.subcategory}</TableCell>
                    <TableCell>{row.complaint_count}</TableCell>
                    <TableCell>{row.affected_vins}</TableCell>
                    <TableCell>{row.avg_ai_confidence ? `${(Number(row.avg_ai_confidence) * 100).toFixed(0)}%` : '-'}</TableCell>
                    <TableCell>{row.mapped_inspection_item || '-'}</TableCell>
                    <TableCell>{row.pdi_catch_rate_pct}%</TableCell>
                    <TableCell>
                      <Chip label={row.gap_classification} color={gapColor(row.gap_classification) as any} size="small" />
                    </TableCell>
                  </TableRow>
                ))}
                {(!fullTable || fullTable.length === 0) && (
                  <TableRow>
                    <TableCell colSpan={8} align="center" sx={{ py: 3 }}>
                      <Typography color="text.secondary">No gap analysis data available</Typography>
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
