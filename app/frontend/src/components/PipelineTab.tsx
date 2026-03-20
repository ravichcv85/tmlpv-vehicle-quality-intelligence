import { useState, useEffect, useRef } from 'react';
import {
  Box, Grid, Button, Typography, Card, CardContent,
  Alert, CircularProgress, Chip, LinearProgress, Stack,
} from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import PlaylistPlayIcon from '@mui/icons-material/PlaylistPlay';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import HourglassTopIcon from '@mui/icons-material/HourglassTop';
import StorageIcon from '@mui/icons-material/Storage';
import { useApi, postApi } from '../hooks/useApi';
import MetricCard from './MetricCard';

interface RunStatus {
  stage: string;
  run_id: number | null;
  status: string;
  result?: string | null;
  message?: string;
}

const STAGES = [
  { key: 'bronze', label: 'Bronze (Lakebase to Bronze)', color: '#cd7f32', desc: 'Ingest raw data from Lakebase staging tables into Delta bronze tables' },
  { key: 'silver', label: 'Silver (Bronze to Silver)', color: '#c0c0c0', desc: 'Clean, join, and enrich bronze tables into silver layer' },
  { key: 'gold', label: 'Gold (Silver to Gold)', color: '#ffd700', desc: 'Aggregate silver tables into gold analytics tables' },
];

export default function PipelineTab() {
  const { data: metrics } = useApi<any>('/api/metrics/summary');
  const [runs, setRuns] = useState<Record<string, RunStatus>>({});
  const [runningAll, setRunningAll] = useState(false);
  const [error, setError] = useState('');
  const pollingRef = useRef<Record<string, number>>({});

  const pollStatus = async (stage: string, runId: number) => {
    try {
      const res = await fetch(`/api/pipeline/status/${runId}`);
      const data = await res.json();
      setRuns(prev => ({
        ...prev,
        [stage]: { stage, run_id: runId, status: data.status, result: data.result, message: data.message },
      }));
      if (data.status === 'TERMINATED' || data.status === 'INTERNAL_ERROR' || data.status === 'SKIPPED') {
        if (pollingRef.current[stage]) {
          clearInterval(pollingRef.current[stage]);
          delete pollingRef.current[stage];
        }
      }
    } catch {
      // ignore polling errors
    }
  };

  const runStage = async (stage: string) => {
    setError('');
    setRuns(prev => ({
      ...prev,
      [stage]: { stage, run_id: null, status: 'SUBMITTING', message: 'Submitting...' },
    }));
    try {
      const res = await postApi<RunStatus>('/api/pipeline/run', { stage });
      setRuns(prev => ({
        ...prev,
        [stage]: res,
      }));
      if (res.run_id) {
        pollingRef.current[stage] = window.setInterval(() => pollStatus(stage, res.run_id!), 5000);
      }
    } catch (err: any) {
      setError(err.message);
      setRuns(prev => ({
        ...prev,
        [stage]: { stage, run_id: null, status: 'ERROR', message: err.message },
      }));
    }
  };

  const runAll = async () => {
    setRunningAll(true);
    setError('');
    try {
      const res = await postApi<RunStatus[]>('/api/pipeline/run', { stage: 'all' });
      const newRuns: Record<string, RunStatus> = {};
      for (const r of res) {
        newRuns[r.stage] = r;
        if (r.run_id) {
          pollingRef.current[r.stage] = window.setInterval(() => pollStatus(r.stage, r.run_id!), 5000);
        }
      }
      setRuns(prev => ({ ...prev, ...newRuns }));
    } catch (err: any) {
      setError(err.message);
    } finally {
      setRunningAll(false);
    }
  };

  useEffect(() => {
    return () => {
      Object.values(pollingRef.current).forEach(id => clearInterval(id));
    };
  }, []);

  const vehicleMetrics = metrics?.vehicles || {};

  const statusIcon = (status: string, result?: string | null) => {
    if (status === 'TERMINATED' && result === 'SUCCESS') return <CheckCircleIcon sx={{ color: '#2e7d32' }} />;
    if (status === 'TERMINATED' && result === 'FAILED') return <ErrorIcon sx={{ color: '#d32f2f' }} />;
    if (status === 'INTERNAL_ERROR') return <ErrorIcon sx={{ color: '#d32f2f' }} />;
    if (status === 'RUNNING' || status === 'PENDING' || status === 'SUBMITTING') return <CircularProgress size={20} />;
    return <HourglassTopIcon sx={{ color: '#9e9e9e' }} />;
  };

  const statusChip = (run: RunStatus) => {
    let color: 'success' | 'error' | 'warning' | 'info' | 'default' = 'default';
    let label = run.status;
    if (run.status === 'TERMINATED' && run.result === 'SUCCESS') { color = 'success'; label = 'SUCCESS'; }
    else if (run.status === 'TERMINATED' && run.result === 'FAILED') { color = 'error'; label = 'FAILED'; }
    else if (run.status === 'RUNNING') { color = 'info'; }
    else if (run.status === 'PENDING' || run.status === 'SUBMITTING' || run.status === 'SUBMITTED') { color = 'warning'; }
    return <Chip label={label} color={color} size="small" />;
  };

  return (
    <Box>
      {/* Metrics */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Total Vehicles"
            value={vehicleMetrics.total_vehicles || 0}
            icon={<StorageIcon fontSize="large" />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="High Risk"
            value={vehicleMetrics.high_risk || 0}
            color="#d32f2f"
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Medium Risk"
            value={vehicleMetrics.medium_risk || 0}
            color="#f57c00"
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Low Risk"
            value={vehicleMetrics.low_risk || 0}
            color="#2e7d32"
          />
        </Grid>
      </Grid>

      {/* Run All */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box display="flex" alignItems="center" justifyContent="space-between">
            <Box>
              <Typography variant="h6">Data Pipeline</Typography>
              <Typography variant="body2" color="text.secondary">
                Run ETL notebooks to refresh gold analytics tables from Lakebase staging data.
              </Typography>
            </Box>
            <Button
              variant="contained"
              size="large"
              onClick={runAll}
              disabled={runningAll}
              startIcon={runningAll ? <CircularProgress size={20} /> : <PlaylistPlayIcon />}
              sx={{ minWidth: 160 }}
            >
              {runningAll ? 'Submitting...' : 'Run All'}
            </Button>
          </Box>
        </CardContent>
      </Card>

      {error && <Alert severity="error" sx={{ mb: 3 }}>{error}</Alert>}

      {/* Individual Stages */}
      <Grid container spacing={2}>
        {STAGES.map(stage => {
          const run = runs[stage.key];
          const isRunning = run && ['RUNNING', 'PENDING', 'SUBMITTING', 'SUBMITTED'].includes(run.status);

          return (
            <Grid item xs={12} md={4} key={stage.key}>
              <Card sx={{ height: '100%', borderTop: `4px solid ${stage.color}` }}>
                <CardContent>
                  <Box display="flex" alignItems="center" justifyContent="space-between" sx={{ mb: 1 }}>
                    <Typography variant="h6" sx={{ fontSize: '1rem' }}>{stage.label}</Typography>
                    {run && statusIcon(run.status, run.result)}
                  </Box>
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                    {stage.desc}
                  </Typography>

                  {isRunning && <LinearProgress sx={{ mb: 2 }} />}

                  {run && (
                    <Stack spacing={1} sx={{ mb: 2 }}>
                      <Box display="flex" alignItems="center" gap={1}>
                        <Typography variant="body2" color="text.secondary">Status:</Typography>
                        {statusChip(run)}
                      </Box>
                      {run.run_id && (
                        <Typography variant="body2" color="text.secondary">
                          Run ID: {run.run_id}
                        </Typography>
                      )}
                      {run.message && (
                        <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.8rem' }}>
                          {run.message}
                        </Typography>
                      )}
                    </Stack>
                  )}

                  <Button
                    variant="outlined"
                    fullWidth
                    onClick={() => runStage(stage.key)}
                    disabled={isRunning}
                    startIcon={isRunning ? <CircularProgress size={16} /> : <PlayArrowIcon />}
                  >
                    {isRunning ? 'Running...' : `Run ${stage.key.charAt(0).toUpperCase() + stage.key.slice(1)}`}
                  </Button>
                </CardContent>
              </Card>
            </Grid>
          );
        })}
      </Grid>
    </Box>
  );
}
