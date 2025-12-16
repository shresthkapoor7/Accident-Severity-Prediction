import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import { ComposableMap, Geographies, Geography } from 'react-simple-maps';
import { MapContainer, TileLayer, CircleMarker, Popup, useMap } from 'react-leaflet';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, LineChart, Line, AreaChart, Area } from 'recharts';
import { 
  LayoutDashboard, Map, Clock, Cloud, Radio, Car, Search, Play, Square,
  MapPin, Thermometer, Wind, Eye, AlertTriangle, CheckCircle, AlertCircle,
  XCircle, Calendar, Building2, TrendingUp, Activity, Zap, BarChart3, Route,
  GitBranch, Gauge
} from 'lucide-react';
import './App.css';

function FlyToMarker({ position }) {
  const map = useMap();
  useEffect(() => {
    if (position) {
      map.flyTo(position, 15, { duration: 1.5 });
    }
  }, [position, map]);
  return null;
}

const API_BASE = '/api';
const geoUrl = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json";

const stateAbbreviations = {
  'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
  'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
  'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
  'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
  'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO',
  'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
  'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
  'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
  'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
  'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
  'District of Columbia': 'DC'
};

const COLORS = ['#17c671', '#ffb400', '#fd7e14', '#c4183c'];

function App() {
  const [currentPage, setCurrentPage] = useState('dashboard');
  const [dateRange, setDateRange] = useState({ min: '', max: '' });
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [loading, setLoading] = useState(false);
  const [summary, setSummary] = useState(null);
  const [stateStats, setStateStats] = useState([]);
  const [timeData, setTimeData] = useState(null);
  const [weatherData, setWeatherData] = useState(null);
  const [hoveredState, setHoveredState] = useState(null);
  const [roadData, setRoadData] = useState(null);
  
  const [streamingActive, setStreamingActive] = useState(false);
  const [streamingPredictions, setStreamingPredictions] = useState([]);
  const [severityStats, setSeverityStats] = useState({ 1: 0, 2: 0, 3: 0, 4: 0 });
  const [selectedStreamState, setSelectedStreamState] = useState(null); // null = all US
  const pollingIntervalRef = useRef(null);

  useEffect(() => {
    fetchDateRange();
    checkStreamingStatus();
    return () => {
      if (pollingIntervalRef.current) clearInterval(pollingIntervalRef.current);
    };
  }, []);

  const fetchDateRange = async () => {
    try {
      const res = await axios.get(`${API_BASE}/date-range`);
      setDateRange(res.data);
      const end = res.data.max_date;
      const start = new Date(end);
      start.setMonth(start.getMonth() - 6);
      setStartDate(start.toISOString().split('T')[0]);
      setEndDate(end);
    } catch (err) { console.error('Error:', err); }
  };

  const checkStreamingStatus = async () => {
    try {
      const res = await axios.get(`${API_BASE}/streaming/status`);
      setStreamingActive(res.data.active);
      if (res.data.active) startPolling();
    } catch (err) { console.error('Error:', err); }
  };

  const fetchLatestPredictions = async () => {
    try {
      const res = await axios.get(`${API_BASE}/streaming/latest?count=50`);
      if (res.data?.length > 0) {
        setStreamingPredictions(res.data);
        updateSeverityStats(res.data);
      }
    } catch (err) { console.error('Error:', err); }
  };

  const updateSeverityStats = (predictions) => {
    const stats = { 1: 0, 2: 0, 3: 0, 4: 0 };
    predictions.forEach(p => { if (p.predicted_severity >= 1 && p.predicted_severity <= 4) stats[p.predicted_severity]++; });
    setSeverityStats(stats);
  };

  const startPolling = () => {
    if (pollingIntervalRef.current) clearInterval(pollingIntervalRef.current);
    fetchLatestPredictions();
    pollingIntervalRef.current = setInterval(fetchLatestPredictions, 1500);
  };

  const startStreaming = async () => {
    try {
      await axios.post(`${API_BASE}/streaming/start`);
      setStreamingActive(true);
      setStreamingPredictions([]);
      setSeverityStats({ 1: 0, 2: 0, 3: 0, 4: 0 });
      startPolling();
    } catch (err) { console.error('Error:', err); }
  };

  const stopStreaming = async () => {
    try {
      await axios.post(`${API_BASE}/streaming/stop`);
      setStreamingActive(false);
      if (pollingIntervalRef.current) clearInterval(pollingIntervalRef.current);
      setStreamingPredictions([]);
      setSeverityStats({ 1: 0, 2: 0, 3: 0, 4: 0 });
    } catch (err) { console.error('Error:', err); }
  };

  const fetchData = async () => {
    setLoading(true);
    try {
      const [summaryRes, stateRes, timeRes, weatherRes, roadRes] = await Promise.all([
        axios.get(`${API_BASE}/summary?start_date=${startDate}&end_date=${endDate}`),
        axios.get(`${API_BASE}/state-stats?start_date=${startDate}&end_date=${endDate}`),
        axios.get(`${API_BASE}/time-analysis?start_date=${startDate}&end_date=${endDate}`),
        axios.get(`${API_BASE}/weather-analysis?start_date=${startDate}&end_date=${endDate}`),
        axios.get(`${API_BASE}/road-analysis?start_date=${startDate}&end_date=${endDate}`)
      ]);
      setSummary(summaryRes.data);
      setStateStats(stateRes.data);
      setTimeData(timeRes.data);
      setWeatherData(weatherRes.data);
      setRoadData(roadRes.data);
    } catch (err) { console.error('Error:', err); }
    setLoading(false);
  };

  const getStateColor = (stateAbbr) => {
    const state = stateStats.find(s => s.state === stateAbbr);
    if (!state) return '#e1e5eb';
    const maxAccidents = Math.max(...stateStats.map(s => s.total_accidents));
    const intensity = state.total_accidents / maxAccidents;
    return `rgba(0, 123, 255, ${0.2 + intensity * 0.8})`;
  };

  const getStateData = (stateAbbr) => stateStats.find(s => s.state === stateAbbr);
  const getSeverityColor = (severity) => COLORS[severity - 1] || '#818ea3';
  
  const getSeverityIcon = (severity) => {
    const icons = [
      <CheckCircle size={18} color="#17c671" />,
      <AlertTriangle size={18} color="#ffb400" />,
      <AlertCircle size={18} color="#fd7e14" />,
      <XCircle size={18} color="#c4183c" />
    ];
    return icons[severity - 1] || <AlertTriangle size={18} />;
  };

  const severityData = summary?.severity_distribution ? 
    Object.entries(summary.severity_distribution).map(([key, value]) => ({ name: `Level ${key}`, value, severity: parseInt(key, 10) })) : [];

  const hourlyData = timeData?.by_hour ? 
    Object.entries(timeData.by_hour).map(([hour, count]) => ({ hour: parseInt(hour, 10), count })).sort((a, b) => a.hour - b.hour) : [];

  const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const weeklyData = timeData?.by_day_of_week ?
    Object.entries(timeData.by_day_of_week).map(([day, count]) => ({ day: dayNames[parseInt(day, 10) - 1], count })) : [];

  const monthlyData = timeData?.by_month ?
    Object.entries(timeData.by_month).map(([month, count]) => ({
      month: parseInt(month, 10),
      label: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][parseInt(month, 10) - 1],
      count
    })).sort((a, b) => a.month - b.month) : [];

  const weatherChartData = weatherData?.weather_conditions ?
    Object.entries(weatherData.weather_conditions).slice(0, 8).map(([condition, count]) => ({
      condition,
      label: condition.length > 18 ? `${condition.substring(0, 16)}…` : condition,
      count
    })) : [];

  const severityByWeatherChartData = weatherData?.severity_by_weather ?
    weatherChartData.map(({ condition, label }) => {
      const breakdown = weatherData.severity_by_weather[condition] || {};
      const s1 = breakdown[1] || breakdown['1'] || 0;
      const s2 = breakdown[2] || breakdown['2'] || 0;
      const s3 = breakdown[3] || breakdown['3'] || 0;
      const s4 = breakdown[4] || breakdown['4'] || 0;
      const total = s1 + s2 + s3 + s4;
      const severeShareLocal = total ? (s3 + s4) / total : 0;
      return {
        condition: label,
        s1,
        s2,
        s3,
        s4,
        total,
        severeShare: severeShareLocal
      };
    }) : [];

  const stateRiskData = stateStats && stateStats.length > 0 ?
    [...stateStats].map(s => {
      const breakdown = s.severity_breakdown || {};
      const s3 = breakdown[3] || breakdown['3'] || 0;
      const s4 = breakdown[4] || breakdown['4'] || 0;
      const severeCount = s3 + s4;
      const total = s.total_accidents || 0;
      return {
        state: s.state,
        total_accidents: total,
        avg_severity: s.avg_severity,
        severeRate: total ? severeCount / total : 0
      };
    }).sort((a, b) => b.severeRate - a.severeRate).slice(0, 10) : [];

  let severeShare = null;
  if (summary?.severity_distribution && summary.total_accidents) {
    const dist = summary.severity_distribution;
    const severeCount = Object.entries(dist).reduce((sum, [sev, value]) => {
      const s = parseInt(sev, 10);
      return s >= 3 ? sum + value : sum;
    }, 0);
    severeShare = severeCount / summary.total_accidents;
  }

  const highestRiskState = stateRiskData && stateRiskData.length > 0 ? stateRiskData[0] : null;

  const peakHour = hourlyData && hourlyData.length > 0
    ? hourlyData.reduce((max, d) => (d.count > max.count ? d : max), hourlyData[0]).hour
    : null;

  let mostCommonCondition = null;
  if (weatherChartData && weatherChartData.length > 0) {
    mostCommonCondition = weatherChartData.reduce((max, d) => (d.count > max.count ? d : max), weatherChartData[0]);
  }

  const highestRiskCondition = severityByWeatherChartData && severityByWeatherChartData.length > 0
    ? [...severityByWeatherChartData].sort((a, b) => b.severeShare - a.severeShare)[0]
    : null;

  const totalWeatherAccidents = weatherChartData?.reduce((sum, d) => sum + d.count, 0) || 0;

  return (
    <div className="app">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="sidebar-brand">
          <div className="sidebar-brand-icon"><Car size={20} color="#fff" /></div>
          <span className="sidebar-brand-text">AccidentAI</span>
        </div>
        <nav className="sidebar-nav">
          <div className={`nav-item ${currentPage === 'dashboard' ? 'active' : ''}`} onClick={() => setCurrentPage('dashboard')}>
            <span className="nav-icon"><LayoutDashboard size={18} /></span>
            <span>Dashboard</span>
          </div>
          <div className={`nav-item ${currentPage === 'map' ? 'active' : ''}`} onClick={() => setCurrentPage('map')}>
            <span className="nav-icon"><Map size={18} /></span>
            <span>US Map</span>
          </div>
          <div className={`nav-item ${currentPage === 'time' ? 'active' : ''}`} onClick={() => setCurrentPage('time')}>
            <span className="nav-icon"><Clock size={18} /></span>
            <span>Time Analysis</span>
          </div>
          <div className={`nav-item ${currentPage === 'weather' ? 'active' : ''}`} onClick={() => setCurrentPage('weather')}>
            <span className="nav-icon"><Cloud size={18} /></span>
            <span>Weather</span>
          </div>
          <div className="nav-divider"></div>
          <div className={`nav-item ${currentPage === 'streaming' ? 'active' : ''}`} onClick={() => setCurrentPage('streaming')}>
            <span className="nav-icon"><Radio size={18} /></span>
            <span>Live Predictions</span>
          </div>
        </nav>
      </aside>

      {/* Main Content */}
      <main className="main-content">
        {/* DASHBOARD PAGE */}
        {currentPage === 'dashboard' && (
          <>
            <div className="page-header">
              <div className="page-header-pretitle">Overview</div>
              <h1 className="page-header-title">Accident Analytics Dashboard</h1>
            </div>

            {/* Controls */}
            <div className="card">
              <div className="card-body">
                <div className="controls-bar">
                  <div className="date-input-group">
                    <label>Start Date</label>
                    <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} />
                  </div>
                  <div className="date-input-group">
                    <label>End Date</label>
                    <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} />
                  </div>
                  <button onClick={fetchData} disabled={loading} className="btn btn-primary">
                    <Search size={16} />
                    {loading ? 'Loading...' : 'Analyze'}
                  </button>
                  <div className="presets-group">
                    <button
                      className="btn btn-outline"
                      onClick={() => {
                        const end = new Date(dateRange.max_date);
                        const start = new Date(end);
                        start.setMonth(start.getMonth() - 1);
                        setStartDate(start.toISOString().split('T')[0]);
                        setEndDate(dateRange.max_date);
                      }}
                    >
                      1M
                    </button>
                    <button
                      className="btn btn-outline"
                      onClick={() => {
                        const end = new Date(dateRange.max_date);
                        const start = new Date(end);
                        start.setMonth(start.getMonth() - 3);
                        setStartDate(start.toISOString().split('T')[0]);
                        setEndDate(dateRange.max_date);
                      }}
                    >
                      3M
                    </button>
                    <button
                      className="btn btn-outline"
                      onClick={() => {
                        const end = new Date(dateRange.max_date);
                        const start = new Date(end);
                        start.setMonth(start.getMonth() - 6);
                        setStartDate(start.toISOString().split('T')[0]);
                        setEndDate(dateRange.max_date);
                      }}
                    >
                      6M
                    </button>
                  </div>
                </div>
              </div>
            </div>

            {/* Primary KPI Stat Cards */}
            {summary && (
              <div className="stats-row">
                <div className="stat-card primary">
                  <div className="stat-label">Total Accidents</div>
                  <div className="stat-value">{summary.total_accidents?.toLocaleString()}</div>
                  <div className="stat-change up"><Activity size={14} /> Included in analysis</div>
                </div>
                <div className="stat-card success">
                  <div className="stat-label">States Covered</div>
                  <div className="stat-value">{summary.states}</div>
                  <div className="stat-change"><Map size={14} /> US states represented</div>
                </div>
                <div className="stat-card warning">
                  <div className="stat-label">Cities</div>
                  <div className="stat-value">{summary.cities?.toLocaleString()}</div>
                  <div className="stat-change"><Building2 size={14} /> Unique locations</div>
                </div>
                <div className="stat-card danger">
                  <div className="stat-label">Date Range</div>
                  <div className="stat-value" style={{ fontSize: '1.2rem' }}>{summary.date_range?.start?.slice(5)}</div>
                  <div className="stat-change"><Calendar size={14} /> to {summary.date_range?.end?.slice(5)}</div>
                </div>
              </div>
            )}

            {/* Advanced KPI Row */}
            {summary && (
              <div className="stats-row">
                {severeShare !== null && (
                  <div className="stat-card info">
                    <div className="stat-label">High-Severity Share</div>
                    <div className="stat-value">{(severeShare * 100).toFixed(1)}%</div>
                    <div className="stat-change"><AlertTriangle size={14} /> Levels 3–4 as share of all accidents</div>
                  </div>
                )}
                {peakHour !== null && (
                  <div className="stat-card primary">
                    <div className="stat-label">Peak Accident Hour</div>
                    <div className="stat-value">{peakHour}:00</div>
                    <div className="stat-change"><Clock size={14} /> Local time with highest volume</div>
                  </div>
                )}
                {highestRiskState && (
                  <div className="stat-card success">
                    <div className="stat-label">Highest Average Severity</div>
                    <div className="stat-value">{highestRiskState.state}</div>
                    <div className="stat-change"><Map size={14} /> Avg severity {highestRiskState.avg_severity.toFixed(2)}</div>
                  </div>
                )}
              </div>
            )}

            {/* Charts Row */}
            <div className="grid-2">
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title"><Activity size={18} style={{ marginRight: 8 }} /> Accidents by Hour</h3>
                </div>
                <div className="card-body">
                  <div className="chart-container">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={hourlyData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e1e5eb" />
                        <XAxis dataKey="hour" stroke="#818ea3" />
                        <YAxis stroke="#818ea3" />
                        <Tooltip />
                        <Area type="monotone" dataKey="count" stroke="#007bff" fill="rgba(0, 123, 255, 0.2)" strokeWidth={2} />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>

              <div className="card">
                <div className="card-header">
                  <h3 className="card-title"><Gauge size={18} style={{ marginRight: 8 }} /> Severity Distribution</h3>
                </div>
                <div className="card-body">
                  <div className="chart-container">
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie data={severityData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={90} label>
                          {severityData.map((entry, index) => (<Cell key={`cell-${index}`} fill={COLORS[index]} />))}
                        </Pie>
                        <Tooltip />
                      </PieChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            </div>

            {/* Road Features Impact */}
            {roadData && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title"><Route size={18} style={{ marginRight: 8 }} /> Road Features Impact</h3>
                  <span style={{ color: '#818ea3', fontSize: '0.9rem' }}>
                    Comparison of accident volume and average severity for key road features
                  </span>
                </div>
                <div className="card-body">
                  <div className="road-grid">
                    {['Crossing', 'Junction', 'Traffic_Signal'].map((featureKey) => {
                      const featureStats = roadData[featureKey] || [];
                      const yesRow = featureStats.find(row => ['true', 'True', '1', 'Yes', 'yes'].includes(String(row.value)));
                      const noRow = featureStats.find(row => ['false', 'False', '0', 'No', 'no'].includes(String(row.value)));
                      const total = featureStats.reduce((sum, row) => sum + row.total_accidents, 0) || 0;

                      const yesShare = yesRow && total ? (yesRow.total_accidents / total) : null;
                      const yesSeverity = yesRow ? yesRow.avg_severity : null;
                      const noSeverity = noRow ? noRow.avg_severity : null;

                      const titleMap = {
                        Crossing: 'Crossings',
                        Junction: 'Junctions',
                        Traffic_Signal: 'Traffic Signals'
                      };

                      const iconMap = {
                        Crossing: <MapPin size={16} />, 
                        Junction: <GitBranch size={16} />, 
                        Traffic_Signal: <Activity size={16} />
                      };

                      return (
                        <div key={featureKey} className="road-feature-card">
                          <div className="road-feature-header">
                            <div className="road-feature-title">
                              <span className="road-feature-icon">{iconMap[featureKey]}</span>
                              <span>{titleMap[featureKey]}</span>
                            </div>
                          </div>
                          <div className="road-feature-body">
                            {yesRow && (
                              <div className="road-feature-metric">
                                <div className="road-feature-label">With feature</div>
                                <div className="road-feature-value">
                                  {yesRow.total_accidents.toLocaleString()} accidents
                                  {yesSeverity != null && (
                                    <span className="road-feature-sub"> · Avg severity {yesSeverity.toFixed(2)}</span>
                                  )}
                                </div>
                              </div>
                            )}
                            {noRow && (
                              <div className="road-feature-metric">
                                <div className="road-feature-label">Without</div>
                                <div className="road-feature-value">
                                  {noRow.total_accidents.toLocaleString()} accidents
                                  {noSeverity != null && (
                                    <span className="road-feature-sub"> · Avg severity {noSeverity.toFixed(2)}</span>
                                  )}
                                </div>
                              </div>
                            )}
                            {yesShare != null && (
                              <div className="road-feature-metric">
                                <div className="road-feature-label">Share of all accidents</div>
                                <div className="road-feature-value">{(yesShare * 100).toFixed(1)}%</div>
                              </div>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            )}
          </>
        )}

        {/* MAP PAGE */}
        {currentPage === 'map' && (
          <>
            <div className="page-header">
              <div className="page-header-pretitle">Geographic Analysis</div>
              <h1 className="page-header-title">US Accidents Map</h1>
            </div>

            <div className="grid-2">
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title"><Map size={18} style={{ marginRight: 8 }} /> Accidents by State</h3>
                  <span style={{ color: '#818ea3', fontSize: '0.9rem' }}>Hover for details</span>
                </div>
                <div className="card-body">
                  <div className="map-wrapper" style={{ position: 'relative' }}>
                    <ComposableMap projection="geoAlbersUsa">
                      <Geographies geography={geoUrl}>
                        {({ geographies }) =>
                          geographies.map(geo => {
                            const stateName = geo.properties.name;
                            const stateAbbr = stateAbbreviations[stateName];
                            return (
                              <Geography
                                key={geo.rsmKey}
                                geography={geo}
                                fill={getStateColor(stateAbbr)}
                                stroke="#fff"
                                strokeWidth={0.5}
                                onMouseEnter={() => setHoveredState(getStateData(stateAbbr))}
                                onMouseLeave={() => setHoveredState(null)}
                                style={{
                                  default: { outline: 'none' },
                                  hover: { outline: 'none', fill: '#007bff' },
                                  pressed: { outline: 'none' }
                                }}
                              />
                            );
                          })
                        }
                      </Geographies>
                    </ComposableMap>
                    
                    {hoveredState && (
                      <div className="state-tooltip">
                        <h4>{hoveredState.state}</h4>
                        <p><Activity size={14} /> Accidents: {hoveredState.total_accidents?.toLocaleString()}</p>
                        <p><AlertTriangle size={14} /> Avg Severity: {hoveredState.avg_severity}</p>
                        <p><Thermometer size={14} /> Avg Temp: {hoveredState.avg_temperature}°F</p>
                        <p><Eye size={14} /> Visibility: {hoveredState.avg_visibility} mi</p>
                      </div>
                    )}
                  </div>
                </div>
              </div>

              <div className="card">
                <div className="card-header">
                  <h3 className="card-title"><BarChart3 size={18} style={{ marginRight: 8 }} /> State Risk Ranking</h3>
                </div>
                <div className="card-body">
                  {stateRiskData.length === 0 ? (
                    <p style={{ color: '#818ea3', fontSize: '0.9rem' }}>Run an analysis on the Dashboard to view state rankings.</p>
                  ) : (
                    <>
                      <div className="chart-container small">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={stateRiskData} layout="vertical">
                            <CartesianGrid strokeDasharray="3 3" stroke="#e1e5eb" />
                            <XAxis type="number" stroke="#818ea3" tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
                            <YAxis dataKey="state" type="category" width={50} stroke="#818ea3" />
                            <Tooltip formatter={(v) => `${(v * 100).toFixed(1)}%`} labelFormatter={(state) => `State ${state}`} />
                            <Bar dataKey="severeRate" fill="#c4183c" radius={[4, 4, 0, 0]} />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                      <div className="state-ranking-table">
                        <div className="state-ranking-header">
                          <span>State</span>
                          <span>Avg Severity</span>
                          <span>High-Severity Share</span>
                        </div>
                        {stateRiskData.map(row => (
                          <div key={row.state} className="state-ranking-row">
                            <span>{row.state}</span>
                            <span>{row.avg_severity.toFixed(2)}</span>
                            <span>{(row.severeRate * 100).toFixed(1)}%</span>
                          </div>
                        ))}
                      </div>
                    </>
                  )}
                </div>
              </div>
            </div>
          </>
        )}

        {/* TIME ANALYSIS PAGE */}
        {currentPage === 'time' && (
          <>
            <div className="page-header">
              <div className="page-header-pretitle">Temporal Patterns</div>
              <h1 className="page-header-title">Time Analysis</h1>
            </div>

            <div className="grid-equal">
              <div className="card">
                <div className="card-header"><h3 className="card-title"><Clock size={18} style={{ marginRight: 8 }} /> Hourly Distribution</h3></div>
                <div className="card-body">
                  <div className="chart-container">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={hourlyData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e1e5eb" />
                        <XAxis dataKey="hour" stroke="#818ea3" />
                        <YAxis stroke="#818ea3" />
                        <Tooltip />
                        <Line type="monotone" dataKey="count" stroke="#007bff" strokeWidth={2} dot={{ fill: '#007bff' }} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>

              <div className="card">
                <div className="card-header"><h3 className="card-title"><Calendar size={18} style={{ marginRight: 8 }} /> Day of Week</h3></div>
                <div className="card-body">
                  <div className="chart-container">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={weeklyData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e1e5eb" />
                        <XAxis dataKey="day" stroke="#818ea3" />
                        <YAxis stroke="#818ea3" />
                        <Tooltip />
                        <Bar dataKey="count" fill="#17c671" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            </div>

            {monthlyData && monthlyData.length > 0 && (
              <div className="card">
                <div className="card-header"><h3 className="card-title"><TrendingUp size={18} style={{ marginRight: 8 }} /> Monthly Trend</h3></div>
                <div className="card-body">
                  <div className="chart-container">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={monthlyData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e1e5eb" />
                        <XAxis dataKey="label" stroke="#818ea3" />
                        <YAxis stroke="#818ea3" />
                        <Tooltip />
                        <Area type="monotone" dataKey="count" stroke="#007bff" fill="rgba(0, 123, 255, 0.2)" strokeWidth={2} />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            )}
          </>
        )}

        {/* WEATHER PAGE */}
        {currentPage === 'weather' && (
          <>
            <div className="page-header">
              <div className="page-header-pretitle">Environmental Factors</div>
              <h1 className="page-header-title">Weather Analysis</h1>
            </div>

            {weatherData && (
              <>
                <div className="stats-row">
                  <div className="stat-card info">
                    <div className="stat-label">Average Temperature</div>
                    <div className="stat-value">{weatherData.temperature.avg}°F</div>
                    <div className="stat-change"><Thermometer size={14} /> Across selected date range</div>
                  </div>
                  <div className="stat-card primary">
                    <div className="stat-label">Minimum Temperature</div>
                    <div className="stat-value">{weatherData.temperature.min}°F</div>
                    <div className="stat-change"><Cloud size={14} /> Coldest observed conditions</div>
                  </div>
                  <div className="stat-card danger">
                    <div className="stat-label">Maximum Temperature</div>
                    <div className="stat-value">{weatherData.temperature.max}°F</div>
                    <div className="stat-change"><Activity size={14} /> Warmest observed conditions</div>
                  </div>
                </div>

                {(mostCommonCondition || highestRiskCondition) && (
                  <div className="stats-row">
                    {mostCommonCondition && (
                      <div className="stat-card primary">
                        <div className="stat-label">Most Common Condition</div>
                        <div className="stat-value" style={{ fontSize: '1.4rem' }}>{mostCommonCondition.label}</div>
                        <div className="stat-change">
                          <Cloud size={14} /> {mostCommonCondition.count.toLocaleString()} accidents
                          {totalWeatherAccidents > 0 && (
                            <span> ({((mostCommonCondition.count / totalWeatherAccidents) * 100).toFixed(1)}%)</span>
                          )}
                        </div>
                      </div>
                    )}
                    {highestRiskCondition && (
                      <div className="stat-card danger">
                        <div className="stat-label">Highest Severity Share</div>
                        <div className="stat-value" style={{ fontSize: '1.4rem' }}>{(highestRiskCondition.severeShare * 100).toFixed(1)}%</div>
                        <div className="stat-change">
                          <AlertTriangle size={14} /> {highestRiskCondition.condition}
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </>
            )}

            <div className="grid-equal">
              <div className="card">
                <div className="card-header"><h3 className="card-title"><Cloud size={18} style={{ marginRight: 8 }} /> Weather Conditions</h3></div>
                <div className="card-body">
                  <div className="chart-container tall">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={weatherChartData} layout="vertical">
                        <CartesianGrid strokeDasharray="3 3" stroke="#e1e5eb" />
                        <XAxis type="number" stroke="#818ea3" />
                        <YAxis dataKey="label" type="category" width={130} stroke="#818ea3" />
                        <Tooltip />
                        <Bar dataKey="count" fill="#00b8d8" radius={[0, 4, 4, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>

              <div className="card">
                <div className="card-header"><h3 className="card-title"><AlertTriangle size={18} style={{ marginRight: 8 }} /> Severity by Weather</h3></div>
                <div className="card-body">
                  <div className="chart-container tall">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={severityByWeatherChartData} layout="vertical">
                        <CartesianGrid strokeDasharray="3 3" stroke="#e1e5eb" />
                        <XAxis type="number" stroke="#818ea3" />
                        <YAxis dataKey="condition" type="category" width={130} stroke="#818ea3" />
                        <Tooltip formatter={(value, name) => {
                          if (name === 's1') return [value, 'Level 1'];
                          if (name === 's2') return [value, 'Level 2'];
                          if (name === 's3') return [value, 'Level 3'];
                          if (name === 's4') return [value, 'Level 4'];
                          if (name === 'severeShare') return [`${(value * 100).toFixed(1)}%`, 'High-severity share'];
                          return value;
                        }} />
                        <Bar dataKey="s1" stackId="a" fill={COLORS[0]} />
                        <Bar dataKey="s2" stackId="a" fill={COLORS[1]} />
                        <Bar dataKey="s3" stackId="a" fill={COLORS[2]} />
                        <Bar dataKey="s4" stackId="a" fill={COLORS[3]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            </div>
          </>
        )}

        {/* STREAMING PAGE */}
        {currentPage === 'streaming' && (
          <>
            <div className="page-header">
              <div className="page-header-pretitle">Real-Time ML Inference</div>
              <h1 className="page-header-title">Live Severity Predictions {selectedStreamState ? `- ${selectedStreamState}` : '- All US'}</h1>
            </div>

            <div className="streaming-status-bar">
              <div className="status-badge">
                <span className={`status-dot ${streamingActive ? 'active' : 'inactive'}`}></span>
                <span>{streamingActive ? 'Streaming Active' : 'Streaming Stopped'}</span>
                {streamingActive && <span style={{ color: '#818ea3', marginLeft: '10px' }}>({streamingPredictions.length} predictions)</span>}
                {selectedStreamState && <span className="live-badge" style={{ marginLeft: '10px', background: '#007bff' }}>{selectedStreamState}</span>}
              </div>
              <div style={{ display: 'flex', gap: '10px' }}>
                {selectedStreamState && (
                  <button onClick={() => setSelectedStreamState(null)} className="btn btn-outline">
                    <Map size={16} /> All US
                  </button>
                )}
                {!streamingActive ? (
                  <button onClick={startStreaming} className="btn btn-success"><Play size={16} /> Start Stream</button>
                ) : (
                  <button onClick={stopStreaming} className="btn btn-danger"><Square size={16} /> Stop Stream</button>
                )}
              </div>
            </div>

            <div className="severity-legend">
              <div className="legend-item"><span className="legend-dot severity-1"></span><span>Level 1: Minor</span></div>
              <div className="legend-item"><span className="legend-dot severity-2"></span><span>Level 2: Moderate</span></div>
              <div className="legend-item"><span className="legend-dot severity-3"></span><span>Level 3: Significant</span></div>
              <div className="legend-item"><span className="legend-dot severity-4"></span><span>Level 4: Severe</span></div>
            </div>

            {/* State Selector Map */}
            <div className="card" style={{ marginBottom: '20px' }}>
              <div className="card-header">
                <h3 className="card-title"><Map size={18} style={{ marginRight: 8 }} /> Select State for Streaming</h3>
                <span style={{ color: '#818ea3', fontSize: '0.9rem' }}>Click a state to filter | Currently: {selectedStreamState || 'All US'}</span>
              </div>
              <div className="card-body" style={{ padding: '10px' }}>
                <ComposableMap projection="geoAlbersUsa" style={{ width: '100%', height: '250px' }}>
                  <Geographies geography={geoUrl}>
                    {({ geographies }) =>
                      geographies.map(geo => {
                        const stateName = geo.properties.name;
                        const stateAbbr = stateAbbreviations[stateName];
                        const isSelected = selectedStreamState === stateAbbr;
                        return (
                          <Geography
                            key={geo.rsmKey}
                            geography={geo}
                            fill={isSelected ? '#007bff' : '#e1e5eb'}
                            stroke="#fff"
                            strokeWidth={0.5}
                            onClick={() => setSelectedStreamState(stateAbbr === selectedStreamState ? null : stateAbbr)}
                            style={{
                              default: { outline: 'none', cursor: 'pointer' },
                              hover: { outline: 'none', fill: '#007bff', cursor: 'pointer' },
                              pressed: { outline: 'none' }
                            }}
                          />
                        );
                      })
                    }
                  </Geographies>
                </ComposableMap>
              </div>
            </div>

            <div className="grid-2">
              <div className="card">
                <div className="card-header"><h3 className="card-title"><MapPin size={18} style={{ marginRight: 8 }} /> US Live Map</h3></div>
                <div className="card-body" style={{ padding: 0 }}>
                  <div className="leaflet-map-container">
                    <MapContainer center={[40.7580, -73.9855]} zoom={12} style={{ height: '400px', width: '100%' }} scrollWheelZoom={true}>
                      <TileLayer attribution='&copy; OpenStreetMap' url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
                      {streamingPredictions.length > 0 && (
                        <FlyToMarker position={[streamingPredictions[streamingPredictions.length - 1].latitude, streamingPredictions[streamingPredictions.length - 1].longitude]} />
                      )}
                      {streamingPredictions.slice(-20).map((pred, idx) => {
                        const isLatest = idx === streamingPredictions.slice(-20).length - 1;
                        return (
                          <CircleMarker key={pred.id} center={[pred.latitude, pred.longitude]} radius={isLatest ? 12 : 6}
                            pathOptions={{ color: '#fff', weight: isLatest ? 2 : 1, fillColor: getSeverityColor(pred.predicted_severity), fillOpacity: isLatest ? 0.9 : 0.6 }}>
                            <Popup>
                              <div className="map-popup">
                                <strong>Level {pred.predicted_severity}</strong><br />
                                {pred.city}, {pred.state}<br />
                                {pred.weather_condition} | {pred.temperature}°F
                              </div>
                            </Popup>
                          </CircleMarker>
                        );
                      })}
                    </MapContainer>
                  </div>
                </div>
              </div>

              <div className="card">
                <div className="card-header"><h3 className="card-title"><Activity size={18} style={{ marginRight: 8 }} /> Severity Stats</h3></div>
                <div className="card-body">
                  <div className="severity-counters">
                    {[1, 2, 3, 4].map(level => (
                      <div key={level} className={`severity-counter level-${level}`}>
                        <div className="counter-icon">{getSeverityIcon(level)}</div>
                        <div className="counter-value">{severityStats[level]}</div>
                        <div className="counter-label">Level {level}</div>
                      </div>
                    ))}
                  </div>
                  <div className="chart-container" style={{ marginTop: '20px' }}>
                    <ResponsiveContainer width="100%" height={150}>
                      <BarChart data={Object.entries(severityStats).map(([level, count]) => ({ name: `L${level}`, value: count }))}>
                        <XAxis dataKey="name" stroke="#818ea3" />
                        <YAxis stroke="#818ea3" />
                        <Bar dataKey="value">
                          {Object.keys(severityStats).map((_, index) => (<Cell key={`cell-${index}`} fill={COLORS[index]} />))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            </div>

            <div className="card">
              <div className="card-header"><h3 className="card-title"><Radio size={18} style={{ marginRight: 8 }} /> Live Feed</h3></div>
              <div className="card-body" style={{ padding: 0 }}>
                <div className="live-feed-container">
                  {streamingPredictions.length === 0 ? (
                    <div style={{ textAlign: 'center', padding: '40px', color: '#818ea3' }}>
                      {streamingActive ? 'Waiting for predictions...' : 'Click "Start Stream" to begin'}
                    </div>
                  ) : (
                    streamingPredictions.slice().reverse().slice(0, 10).map((pred) => (
                      <div key={pred.id} className="feed-item">
                        <div className={`feed-severity level-${pred.predicted_severity}`}>{getSeverityIcon(pred.predicted_severity)}</div>
                        <div className="feed-content">
                          <div className="feed-title">Severity Level {pred.predicted_severity} {pred.weather_source && <span className="live-badge">LIVE</span>}</div>
                          <div className="feed-location"><MapPin size={12} /> {pred.neighborhood || pred.city} • {pred.street}</div>
                          <div className="feed-meta">
                            <span><Cloud size={12} /> {pred.weather_condition}</span>
                            <span><Thermometer size={12} /> {pred.temperature}°F</span>
                            <span><Wind size={12} /> {pred.wind_speed} mph</span>
                          </div>
                        </div>
                        <div className="feed-time">{new Date(pred.timestamp).toLocaleTimeString()}</div>
                      </div>
                    ))
                  )}
                </div>
              </div>
            </div>

            <div className="info-box">
              <h4><Zap size={16} style={{ marginRight: 8 }} /> Architecture</h4>
              <p><strong>Data Flow:</strong> OpenWeatherMap API → Kafka Topics (51 states) → Spark ML Model → React Dashboard</p>
              <p><strong>Topics:</strong> accident-{'{state}'} (for example, accident-ca, accident-ny) plus accident-all and accident-weather-adverse</p>
              <p>Click a state above to filter the stream to that state's Kafka topic only.</p>
            </div>
          </>
        )}
      </main>
    </div>
  );
}

export default App;
