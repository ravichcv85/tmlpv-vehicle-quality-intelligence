import { useState } from 'react';
import {
  ThemeProvider, CssBaseline, AppBar, Toolbar, Typography, Box,
  Tabs, Tab, Container,
} from '@mui/material';
import DirectionsCarIcon from '@mui/icons-material/DirectionsCar';
import SupportAgentIcon from '@mui/icons-material/SupportAgent';
import AssignmentIcon from '@mui/icons-material/Assignment';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import theme from './theme';
import CRMTab from './components/CRMTab';
import PDITab from './components/PDITab';
import ChecklistAgentTab from './components/ChecklistAgentTab';
import PipelineTab from './components/PipelineTab';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel({ children, value, index }: TabPanelProps) {
  return (
    <div role="tabpanel" hidden={value !== index}>
      {value === index && <Box sx={{ pt: 3 }}>{children}</Box>}
    </div>
  );
}

function App() {
  const [tab, setTab] = useState(0);

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
        {/* Header */}
        <AppBar position="static" sx={{ background: 'linear-gradient(135deg, #1a237e 0%, #0d47a1 100%)' }}>
          <Toolbar>
            <DirectionsCarIcon sx={{ mr: 1.5, fontSize: 28 }} />
            <Box sx={{ flexGrow: 1 }}>
              <Typography variant="h6" sx={{ fontWeight: 700, lineHeight: 1.2 }}>
                TMLPV Vehicle Quality Intelligence
              </Typography>
              <Typography variant="caption" sx={{ opacity: 0.8 }}>
                Tata Motors Limited - Passenger Vehicles
              </Typography>
            </Box>
            <Box sx={{ display: { xs: 'none', md: 'flex' }, alignItems: 'center', gap: 1 }}>
              <Typography variant="body2" sx={{ opacity: 0.7 }}>
                Powered by Databricks
              </Typography>
            </Box>
          </Toolbar>
        </AppBar>

        {/* Tabs */}
        <Box sx={{ bgcolor: 'white', borderBottom: 1, borderColor: 'divider', boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}>
          <Container maxWidth="xl">
            <Tabs
              value={tab}
              onChange={(_, v) => setTab(v)}
              variant="scrollable"
              scrollButtons="auto"
              sx={{
                '& .MuiTab-root': {
                  minHeight: 56,
                  fontWeight: 600,
                  fontSize: '0.85rem',
                },
              }}
            >
              <Tab icon={<SupportAgentIcon />} iconPosition="start" label="CRM - Log Complaint" />
              <Tab icon={<AssignmentIcon />} iconPosition="start" label="PDI Tablet" />
              <Tab icon={<SmartToyIcon />} iconPosition="start" label="AI Checklist Agent" />
              <Tab icon={<AccountTreeIcon />} iconPosition="start" label="Run Pipeline" />
            </Tabs>
          </Container>
        </Box>

        {/* Content */}
        <Container maxWidth="xl" sx={{ py: 3 }}>
          <TabPanel value={tab} index={0}>
            <CRMTab />
          </TabPanel>
          <TabPanel value={tab} index={1}>
            <PDITab />
          </TabPanel>
          <TabPanel value={tab} index={2}>
            <ChecklistAgentTab />
          </TabPanel>
          <TabPanel value={tab} index={3}>
            <PipelineTab />
          </TabPanel>
        </Container>
      </Box>
    </ThemeProvider>
  );
}

export default App;
