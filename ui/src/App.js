import React, { useState, useEffect } from 'react';
import {
  Database,
  Play,
  Upload,
  Search,
  MessageSquare,
  Table,
  Settings,
  RefreshCw,
  Plus,
  FileText,
  BarChart3,
  ChevronRight,
  AlertCircle,
  CheckCircle,
  TrendingUp,
  Clock,
  Layers,
  GitBranch,
  Zap,
  Calendar,
  Code
} from 'lucide-react';
import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

// API client
const api = axios.create({
  baseURL: API_URL,
  headers: { 'Content-Type': 'application/json' }
});

// Tab navigation component
function TabNav({ activeTab, setActiveTab }) {
  const tabs = [
    { id: 'data', label: 'Data Explorer', icon: Database },
    { id: 'sql', label: 'SQL Editor', icon: FileText },
    { id: 'pipelines', label: 'Pipelines', icon: GitBranch },
    { id: 'transforms', label: 'Transforms', icon: Layers },
    { id: 'jobs', label: 'Jobs', icon: Play },
    { id: 'ask', label: 'Ask Data', icon: MessageSquare },
  ];

  return (
    <nav className="tab-nav">
      {tabs.map(tab => (
        <button
          key={tab.id}
          className={`tab-button ${activeTab === tab.id ? 'active' : ''}`}
          onClick={() => setActiveTab(tab.id)}
        >
          <tab.icon size={18} />
          <span>{tab.label}</span>
        </button>
      ))}
    </nav>
  );
}

// Data Explorer component
function DataExplorer() {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState(null);
  const [tableData, setTableData] = useState(null);
  const [tableInfo, setTableInfo] = useState(null);
  const [loading, setLoading] = useState(false);
  const [uploadFile, setUploadFile] = useState(null);
  const [uploadTableName, setUploadTableName] = useState('');

  useEffect(() => {
    fetchTables();
  }, []);

  const fetchTables = async () => {
    try {
      const response = await api.get('/tables');
      setTables(response.data.tables || []);
    } catch (error) {
      console.error('Error fetching tables:', error);
    }
  };

  const selectTable = async (tableName) => {
    setSelectedTable(tableName);
    setLoading(true);
    try {
      const [infoRes, dataRes] = await Promise.all([
        api.get(`/tables/${tableName}`),
        api.get(`/tables/${tableName}/preview`)
      ]);
      setTableInfo(infoRes.data);
      setTableData(dataRes.data);
    } catch (error) {
      console.error('Error fetching table data:', error);
    }
    setLoading(false);
  };

  const handleUpload = async () => {
    if (!uploadFile || !uploadTableName) return;

    const formData = new FormData();
    formData.append('file', uploadFile);

    try {
      await api.post(`/upload?table_name=${uploadTableName}`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      });
      setUploadFile(null);
      setUploadTableName('');
      fetchTables();
    } catch (error) {
      console.error('Error uploading file:', error);
    }
  };

  return (
    <div className="data-explorer">
      <div className="sidebar">
        <div className="sidebar-header">
          <h3>Tables</h3>
          <button onClick={fetchTables} className="icon-button">
            <RefreshCw size={16} />
          </button>
        </div>

        <div className="table-list">
          {tables.length === 0 ? (
            <p className="no-data">No tables found</p>
          ) : (
            tables.map(table => (
              <div
                key={table}
                className={`table-item ${selectedTable === table ? 'selected' : ''}`}
                onClick={() => selectTable(table)}
              >
                <Table size={16} />
                <span>{table}</span>
                <ChevronRight size={14} />
              </div>
            ))
          )}
        </div>

        <div className="upload-section">
          <h4>Upload Data</h4>
          <input
            type="text"
            placeholder="Table name"
            value={uploadTableName}
            onChange={(e) => setUploadTableName(e.target.value)}
          />
          <input
            type="file"
            accept=".csv,.json,.parquet"
            onChange={(e) => setUploadFile(e.target.files[0])}
          />
          <button onClick={handleUpload} disabled={!uploadFile || !uploadTableName}>
            <Upload size={16} />
            Upload
          </button>
        </div>
      </div>

      <div className="main-content">
        {loading ? (
          <div className="loading">Loading...</div>
        ) : selectedTable && tableData ? (
          <>
            <div className="table-header">
              <h2>{selectedTable}</h2>
              {tableInfo && (
                <span className="row-count">{tableInfo.row_count.toLocaleString()} rows</span>
              )}
            </div>

            {tableInfo && (
              <div className="column-info">
                <strong>Columns:</strong>
                {tableInfo.columns.map(col => (
                  <span key={col.name} className="column-badge">
                    {col.name} <small>({col.type})</small>
                  </span>
                ))}
              </div>
            )}

            <div className="data-table-container">
              <table className="data-table">
                <thead>
                  <tr>
                    {tableData.columns.map(col => (
                      <th key={col}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {tableData.data.map((row, i) => (
                    <tr key={i}>
                      {tableData.columns.map(col => (
                        <td key={col}>{String(row[col] ?? '')}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : (
          <div className="empty-state">
            <Database size={48} />
            <h3>Select a table to view data</h3>
            <p>Or upload a new file to create a table</p>
          </div>
        )}
      </div>
    </div>
  );
}

// SQL Editor component
function SQLEditor() {
  const [query, setQuery] = useState('SELECT * FROM ');
  const [results, setResults] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const executeQuery = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await api.post('/query', { query });
      setResults(response.data);
    } catch (err) {
      setError(err.response?.data?.detail || 'Query execution failed');
      setResults(null);
    }
    setLoading(false);
  };

  return (
    <div className="sql-editor">
      <div className="editor-section">
        <div className="editor-header">
          <h3>SQL Query</h3>
          <button onClick={executeQuery} disabled={loading} className="run-button">
            <Play size={16} />
            {loading ? 'Running...' : 'Run Query'}
          </button>
        </div>
        <textarea
          className="query-input"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Enter your SQL query..."
          onKeyDown={(e) => {
            if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
              executeQuery();
            }
          }}
        />
        <small className="hint">Press Ctrl+Enter to execute</small>
      </div>

      <div className="results-section">
        {error && (
          <div className="error-message">
            <AlertCircle size={16} />
            {error}
          </div>
        )}

        {results && (
          <>
            <div className="results-header">
              <h3>Results</h3>
              {results.row_count !== undefined && (
                <span>{results.row_count} rows returned</span>
              )}
            </div>

            {results.data ? (
              <div className="data-table-container">
                <table className="data-table">
                  <thead>
                    <tr>
                      {results.columns.map(col => (
                        <th key={col}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {results.data.map((row, i) => (
                      <tr key={i}>
                        {results.columns.map(col => (
                          <td key={col}>{String(row[col] ?? '')}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="success-message">
                <CheckCircle size={16} />
                {results.message}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

// Pipeline Creator component (Yahoo Finance)
function PipelineCreator() {
  const [mode, setMode] = useState('fetch'); // 'fetch' or 'schedule'
  const [symbols, setSymbols] = useState('AAPL, GOOGL, MSFT');
  const [period, setPeriod] = useState('1mo');
  const [scheduleMode, setScheduleMode] = useState('natural'); // 'natural', 'visual', 'manual'
  const [naturalSchedule, setNaturalSchedule] = useState('weekdays at 6pm');
  const [cronExpression, setCronExpression] = useState('0 18 * * 1-5');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [previewData, setPreviewData] = useState(null);
  const [cronExamples, setCronExamples] = useState([]);

  useEffect(() => {
    // Load cron examples on mount
    api.get('/cron/examples').then(res => {
      setCronExamples(res.data.examples || []);
    }).catch(() => {});
  }, []);

  const parseSymbols = () => {
    return symbols.split(',').map(s => s.trim().toUpperCase()).filter(s => s);
  };

  const fetchNow = async () => {
    setLoading(true);
    setError(null);
    setResult(null);
    setPreviewData(null);

    try {
      const response = await api.post('/yahoo-finance/fetch', {
        symbols: parseSymbols(),
        period
      });
      setPreviewData(response.data);
      setResult({ type: 'preview', message: `Fetched ${response.data.row_count} records for ${response.data.symbols_fetched.join(', ')}` });
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to fetch data');
    }
    setLoading(false);
  };

  const saveData = async () => {
    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await api.post('/yahoo-finance/save', {
        symbols: parseSymbols(),
        period
      });
      setResult({ type: 'success', message: response.data.message });
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to save data');
    }
    setLoading(false);
  };

  const parseCron = async () => {
    if (scheduleMode !== 'natural') return;

    try {
      const response = await api.post('/cron/parse', {
        natural_language: naturalSchedule
      });
      setCronExpression(response.data.cron);
    } catch (err) {
      setError('Could not parse schedule: ' + (err.response?.data?.detail || err.message));
    }
  };

  const createPipeline = async () => {
    setLoading(true);
    setError(null);
    setResult(null);

    // Parse cron if using natural language
    if (scheduleMode === 'natural') {
      await parseCron();
    }

    try {
      const response = await api.post('/yahoo-finance/create-pipeline', {
        symbols: parseSymbols(),
        schedule: cronExpression,
        period
      });
      setResult({ type: 'success', message: `Pipeline created! DAG ID: ${response.data.dag_id}` });
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to create pipeline');
    }
    setLoading(false);
  };

  return (
    <div className="pipeline-creator">
      <div className="creator-header">
        <h2><TrendingUp size={24} /> Yahoo Finance Pipeline</h2>
        <p>Fetch stock market data and create scheduled pipelines</p>
      </div>

      <div className="mode-toggle">
        <button
          className={mode === 'fetch' ? 'active' : ''}
          onClick={() => setMode('fetch')}
        >
          <Zap size={16} /> Fetch Now
        </button>
        <button
          className={mode === 'schedule' ? 'active' : ''}
          onClick={() => setMode('schedule')}
        >
          <Calendar size={16} /> Schedule Pipeline
        </button>
      </div>

      <div className="form-section">
        <div className="form-group">
          <label>Stock Symbols (comma-separated)</label>
          <input
            type="text"
            value={symbols}
            onChange={(e) => setSymbols(e.target.value)}
            placeholder="AAPL, GOOGL, MSFT, AMZN"
          />
          <small>Enter valid stock ticker symbols</small>
        </div>

        <div className="form-group">
          <label>Data Period</label>
          <select value={period} onChange={(e) => setPeriod(e.target.value)}>
            <option value="1d">1 Day</option>
            <option value="5d">5 Days</option>
            <option value="1mo">1 Month</option>
            <option value="3mo">3 Months</option>
            <option value="6mo">6 Months</option>
            <option value="1y">1 Year</option>
            <option value="2y">2 Years</option>
            <option value="5y">5 Years</option>
            <option value="max">All Available</option>
          </select>
        </div>

        {mode === 'schedule' && (
          <div className="schedule-section">
            <div className="form-group">
              <label>Schedule Mode</label>
              <div className="schedule-mode-toggle">
                <button
                  className={scheduleMode === 'natural' ? 'active' : ''}
                  onClick={() => setScheduleMode('natural')}
                >
                  Natural Language
                </button>
                <button
                  className={scheduleMode === 'visual' ? 'active' : ''}
                  onClick={() => setScheduleMode('visual')}
                >
                  Visual Builder
                </button>
                <button
                  className={scheduleMode === 'manual' ? 'active' : ''}
                  onClick={() => setScheduleMode('manual')}
                >
                  Manual Cron
                </button>
              </div>
            </div>

            {scheduleMode === 'natural' && (
              <div className="form-group">
                <label>When should this run?</label>
                <input
                  type="text"
                  value={naturalSchedule}
                  onChange={(e) => setNaturalSchedule(e.target.value)}
                  placeholder="e.g., weekdays at 6pm"
                  onBlur={parseCron}
                />
                <small>Examples: "daily at 5pm", "every monday at 9am", "every 2 hours"</small>
                {cronExamples.length > 0 && (
                  <div className="cron-examples">
                    {cronExamples.slice(0, 4).map((ex, i) => (
                      <span
                        key={i}
                        className="example-chip"
                        onClick={() => {
                          setNaturalSchedule(ex.natural);
                          setCronExpression(ex.cron);
                        }}
                      >
                        {ex.natural}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            )}

            {scheduleMode === 'visual' && (
              <div className="visual-builder">
                <div className="form-group">
                  <label>Run at (hour)</label>
                  <select onChange={(e) => setCronExpression(`0 ${e.target.value} * * 1-5`)}>
                    {[...Array(24)].map((_, i) => (
                      <option key={i} value={i}>{i}:00</option>
                    ))}
                  </select>
                </div>
                <div className="form-group">
                  <label>Days</label>
                  <select onChange={(e) => {
                    const hour = cronExpression.split(' ')[1] || '9';
                    setCronExpression(`0 ${hour} * * ${e.target.value}`);
                  }}>
                    <option value="*">Every day</option>
                    <option value="1-5">Weekdays (Mon-Fri)</option>
                    <option value="0,6">Weekends (Sat-Sun)</option>
                    <option value="1">Mondays only</option>
                  </select>
                </div>
              </div>
            )}

            {scheduleMode === 'manual' && (
              <div className="form-group">
                <label>Cron Expression</label>
                <input
                  type="text"
                  value={cronExpression}
                  onChange={(e) => setCronExpression(e.target.value)}
                  placeholder="0 18 * * 1-5"
                />
                <small>Format: minute hour day-of-month month day-of-week</small>
              </div>
            )}

            <div className="cron-preview">
              <Code size={16} />
              <span>Cron: <code>{cronExpression}</code></span>
            </div>
          </div>
        )}
      </div>

      <div className="form-actions">
        {mode === 'fetch' ? (
          <>
            <button onClick={fetchNow} disabled={loading} className="secondary">
              <Play size={16} />
              {loading ? 'Fetching...' : 'Preview Data'}
            </button>
            <button onClick={saveData} disabled={loading || !previewData} className="primary">
              <Database size={16} />
              {loading ? 'Saving...' : 'Fetch & Save to DuckDB'}
            </button>
          </>
        ) : (
          <button onClick={createPipeline} disabled={loading} className="primary">
            <Plus size={16} />
            {loading ? 'Creating...' : 'Create Scheduled Pipeline'}
          </button>
        )}
      </div>

      {error && (
        <div className="error-message">
          <AlertCircle size={16} />
          {error}
        </div>
      )}

      {result && (
        <div className={`result-message ${result.type}`}>
          <CheckCircle size={16} />
          {result.message}
        </div>
      )}

      {previewData && (
        <div className="preview-section">
          <h3>Preview Data ({previewData.row_count} records)</h3>
          <div className="data-table-container">
            <table className="data-table">
              <thead>
                <tr>
                  {previewData.columns.map(col => (
                    <th key={col}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {previewData.data.slice(0, 20).map((row, i) => (
                  <tr key={i}>
                    {previewData.columns.map(col => (
                      <td key={col}>{String(row[col] ?? '').substring(0, 50)}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {previewData.data.length > 20 && (
            <p className="truncated">Showing first 20 of {previewData.data.length} rows</p>
          )}
        </div>
      )}
    </div>
  );
}

// Transforms component (DBT Templates)
function Transforms() {
  const [templates, setTemplates] = useState([]);
  const [selectedTemplate, setSelectedTemplate] = useState(null);
  const [tables, setTables] = useState([]);
  const [tableColumns, setTableColumns] = useState([]);
  const [sourceTable, setSourceTable] = useState('');
  const [outputName, setOutputName] = useState('');
  const [columnMappings, setColumnMappings] = useState({});
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [existingModels, setExistingModels] = useState([]);

  useEffect(() => {
    loadTemplates();
    loadTables();
    loadExistingModels();
  }, []);

  const loadTemplates = async () => {
    try {
      const response = await api.get('/dbt/templates');
      setTemplates(response.data.templates || []);
    } catch (err) {
      console.error('Error loading templates:', err);
    }
  };

  const loadTables = async () => {
    try {
      const response = await api.get('/tables');
      setTables(response.data.tables || []);
    } catch (err) {
      console.error('Error loading tables:', err);
    }
  };

  const loadExistingModels = async () => {
    try {
      const response = await api.get('/dbt/models');
      setExistingModels(response.data.models || []);
    } catch (err) {
      console.error('Error loading models:', err);
    }
  };

  const loadTableColumns = async (tableName) => {
    try {
      const response = await api.get(`/tables/${tableName}`);
      setTableColumns(response.data.columns || []);
    } catch (err) {
      console.error('Error loading columns:', err);
    }
  };

  const handleSelectTemplate = async (template) => {
    setSelectedTemplate(template);
    setColumnMappings({});
    setResult(null);
    setError(null);

    // Load template details
    try {
      const response = await api.get(`/dbt/templates/${template.id}`);
      setSelectedTemplate(response.data);
    } catch (err) {
      console.error('Error loading template:', err);
    }
  };

  const handleSourceTableChange = (tableName) => {
    setSourceTable(tableName);
    loadTableColumns(tableName);
    setOutputName(`${tableName}_${selectedTemplate?.id || 'transform'}`);
  };

  const createModel = async () => {
    if (!selectedTemplate || !sourceTable || !outputName) {
      setError('Please fill in all required fields');
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await api.post('/dbt/create-model', {
        template_id: selectedTemplate.id,
        source_table: sourceTable,
        output_model_name: outputName,
        column_mappings: columnMappings
      });
      setResult(response.data);
      loadExistingModels();
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to create model');
    }
    setLoading(false);
  };

  const runDbt = async () => {
    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await api.post('/dbt/run');
      if (response.data.success) {
        setResult({ message: 'DBT run completed successfully!' });
      } else {
        setError('DBT run failed: ' + response.data.stderr);
      }
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to run DBT');
    }
    setLoading(false);
  };

  return (
    <div className="transforms">
      <div className="transforms-header">
        <h2><Layers size={24} /> Data Transformations</h2>
        <p>Create DBT models from pre-built templates</p>
      </div>

      <div className="transforms-content">
        <div className="templates-list">
          <h3>Available Templates</h3>
          {templates.map(template => (
            <div
              key={template.id}
              className={`template-card ${selectedTemplate?.id === template.id ? 'selected' : ''}`}
              onClick={() => handleSelectTemplate(template)}
            >
              <h4>{template.name}</h4>
              <p>{template.description}</p>
              <div className="template-meta">
                <span>{template.required_columns.length} parameters</span>
              </div>
            </div>
          ))}
        </div>

        <div className="template-config">
          {selectedTemplate ? (
            <>
              <h3>Configure: {selectedTemplate.name}</h3>
              <p>{selectedTemplate.description}</p>

              <div className="form-section">
                <div className="form-group">
                  <label>Source Table</label>
                  <select
                    value={sourceTable}
                    onChange={(e) => handleSourceTableChange(e.target.value)}
                  >
                    <option value="">Select a table...</option>
                    {tables.map(table => (
                      <option key={table} value={table}>{table}</option>
                    ))}
                  </select>
                </div>

                <div className="form-group">
                  <label>Output Model Name</label>
                  <input
                    type="text"
                    value={outputName}
                    onChange={(e) => setOutputName(e.target.value)}
                    placeholder="my_transform"
                  />
                </div>

                {selectedTemplate.required_columns && (
                  <div className="column-mappings">
                    <h4>Map Columns</h4>
                    {selectedTemplate.required_columns.map(param => (
                      <div key={param} className="form-group">
                        <label>{param.replace(/_/g, ' ')}</label>
                        {param === 'window_size' ? (
                          <input
                            type="number"
                            value={columnMappings[param] || ''}
                            onChange={(e) => setColumnMappings({...columnMappings, [param]: e.target.value})}
                            placeholder="e.g., 7 for 7-day window"
                          />
                        ) : (
                          <select
                            value={columnMappings[param] || ''}
                            onChange={(e) => setColumnMappings({...columnMappings, [param]: e.target.value})}
                          >
                            <option value="">Select column...</option>
                            {tableColumns.map(col => (
                              <option key={col.name} value={col.name}>
                                {col.name} ({col.type})
                              </option>
                            ))}
                          </select>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div className="form-actions">
                <button onClick={createModel} disabled={loading} className="primary">
                  <Plus size={16} />
                  {loading ? 'Creating...' : 'Create Model'}
                </button>
                <button onClick={runDbt} disabled={loading} className="secondary">
                  <Play size={16} />
                  Run DBT
                </button>
              </div>
            </>
          ) : (
            <div className="empty-state">
              <Layers size={48} />
              <h3>Select a template</h3>
              <p>Choose a transformation template to get started</p>
            </div>
          )}

          {error && (
            <div className="error-message">
              <AlertCircle size={16} />
              {error}
            </div>
          )}

          {result && (
            <div className="result-message success">
              <CheckCircle size={16} />
              <div>
                <p>{result.message}</p>
                {result.sql_preview && (
                  <pre className="sql-preview">{result.sql_preview}</pre>
                )}
              </div>
            </div>
          )}
        </div>

        <div className="existing-models">
          <h3>Existing Models</h3>
          {existingModels.length === 0 ? (
            <p className="no-data">No models created yet</p>
          ) : (
            <div className="models-list">
              {existingModels.map(model => (
                <div key={model.name} className="model-item">
                  <FileText size={16} />
                  <span>{model.name}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// Jobs component (Airflow integration)
function Jobs() {
  const [dags, setDags] = useState([]);
  const [selectedDag, setSelectedDag] = useState(null);
  const [dagRuns, setDagRuns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newDag, setNewDag] = useState({
    dag_id: '',
    description: '',
    schedule: '@daily',
    source_type: 'api',
    source_config: { url: '', data_key: '' },
    target_table: ''
  });

  useEffect(() => {
    fetchDags();
  }, []);

  const fetchDags = async () => {
    setLoading(true);
    try {
      const response = await api.get('/dags');
      setDags(response.data.dags || []);
    } catch (error) {
      console.error('Error fetching DAGs:', error);
    }
    setLoading(false);
  };

  const selectDag = async (dagId) => {
    setSelectedDag(dagId);
    try {
      const response = await api.get(`/dags/${dagId}/runs`);
      setDagRuns(response.data.dag_runs || []);
    } catch (error) {
      console.error('Error fetching DAG runs:', error);
    }
  };

  const triggerDag = async (dagId) => {
    try {
      await api.post(`/dags/${dagId}/trigger`);
      selectDag(dagId);
    } catch (error) {
      console.error('Error triggering DAG:', error);
    }
  };

  const createDag = async () => {
    try {
      await api.post('/dags/create', newDag);
      setShowCreateForm(false);
      setNewDag({
        dag_id: '',
        description: '',
        schedule: '@daily',
        source_type: 'api',
        source_config: { url: '', data_key: '' },
        target_table: ''
      });
      fetchDags();
    } catch (error) {
      console.error('Error creating DAG:', error);
    }
  };

  return (
    <div className="jobs-view">
      <div className="jobs-header">
        <h2>Data Pipelines</h2>
        <button onClick={() => setShowCreateForm(true)} className="create-button">
          <Plus size={16} />
          New Pipeline
        </button>
      </div>

      {showCreateForm && (
        <div className="create-form">
          <h3>Create New Pipeline</h3>
          <div className="form-grid">
            <input
              type="text"
              placeholder="Pipeline ID (e.g., load_gleif_data)"
              value={newDag.dag_id}
              onChange={(e) => setNewDag({...newDag, dag_id: e.target.value})}
            />
            <input
              type="text"
              placeholder="Description"
              value={newDag.description}
              onChange={(e) => setNewDag({...newDag, description: e.target.value})}
            />
            <select
              value={newDag.schedule}
              onChange={(e) => setNewDag({...newDag, schedule: e.target.value})}
            >
              <option value="@once">Run Once</option>
              <option value="@hourly">Hourly</option>
              <option value="@daily">Daily</option>
              <option value="@weekly">Weekly</option>
              <option value="@monthly">Monthly</option>
            </select>
            <select
              value={newDag.source_type}
              onChange={(e) => setNewDag({...newDag, source_type: e.target.value})}
            >
              <option value="api">API</option>
              <option value="file">File</option>
            </select>
            {newDag.source_type === 'api' && (
              <>
                <input
                  type="text"
                  placeholder="API URL"
                  value={newDag.source_config.url}
                  onChange={(e) => setNewDag({
                    ...newDag,
                    source_config: {...newDag.source_config, url: e.target.value}
                  })}
                />
                <input
                  type="text"
                  placeholder="Data key (optional, e.g., 'data' or 'results')"
                  value={newDag.source_config.data_key}
                  onChange={(e) => setNewDag({
                    ...newDag,
                    source_config: {...newDag.source_config, data_key: e.target.value}
                  })}
                />
              </>
            )}
            <input
              type="text"
              placeholder="Target table name"
              value={newDag.target_table}
              onChange={(e) => setNewDag({...newDag, target_table: e.target.value})}
            />
          </div>
          <div className="form-actions">
            <button onClick={() => setShowCreateForm(false)}>Cancel</button>
            <button onClick={createDag} className="primary">Create Pipeline</button>
          </div>
        </div>
      )}

      <div className="dags-list">
        {loading ? (
          <div className="loading">Loading pipelines...</div>
        ) : dags.length === 0 ? (
          <div className="empty-state">
            <BarChart3 size={48} />
            <h3>No pipelines found</h3>
            <p>Create your first data pipeline to get started</p>
          </div>
        ) : (
          dags.map(dag => (
            <div
              key={dag.dag_id}
              className={`dag-card ${selectedDag === dag.dag_id ? 'selected' : ''}`}
              onClick={() => selectDag(dag.dag_id)}
            >
              <div className="dag-info">
                <h4>{dag.dag_id}</h4>
                <p>{dag.description}</p>
                <span className={`status ${dag.is_paused ? 'paused' : 'active'}`}>
                  {dag.is_paused ? 'Paused' : 'Active'}
                </span>
              </div>
              <button
                onClick={(e) => { e.stopPropagation(); triggerDag(dag.dag_id); }}
                className="trigger-button"
              >
                <Play size={16} />
                Run
              </button>
            </div>
          ))
        )}
      </div>

      {selectedDag && dagRuns.length > 0 && (
        <div className="dag-runs">
          <h3>Recent Runs - {selectedDag}</h3>
          <table className="runs-table">
            <thead>
              <tr>
                <th>Run ID</th>
                <th>State</th>
                <th>Start Date</th>
                <th>End Date</th>
              </tr>
            </thead>
            <tbody>
              {dagRuns.map(run => (
                <tr key={run.dag_run_id}>
                  <td>{run.dag_run_id}</td>
                  <td>
                    <span className={`state ${run.state}`}>{run.state}</span>
                  </td>
                  <td>{run.start_date ? new Date(run.start_date).toLocaleString() : '-'}</td>
                  <td>{run.end_date ? new Date(run.end_date).toLocaleString() : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// Ask Data component (Semantic Search + AI)
function AskData() {
  const [question, setQuestion] = useState('');
  const [answer, setAnswer] = useState(null);
  const [loading, setLoading] = useState(false);
  const [tables, setTables] = useState([]);
  const [vectorizedTables, setVectorizedTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [tableColumns, setTableColumns] = useState([]);
  const [ollamaStatus, setOllamaStatus] = useState(null);
  const [mode, setMode] = useState('semantic'); // 'semantic' or 'ai'

  useEffect(() => {
    fetchTables();
    checkOllamaStatus();
  }, []);

  const fetchTables = async () => {
    try {
      const response = await api.get('/tables');
      setTables(response.data.tables || []);
    } catch (error) {
      console.error('Error fetching tables:', error);
    }
  };

  const checkOllamaStatus = async () => {
    try {
      const response = await api.get('/ollama/status');
      setOllamaStatus(response.data);
    } catch (error) {
      console.error('Error checking Ollama status:', error);
    }
  };

  const fetchTableColumns = async (tableName) => {
    try {
      const response = await api.get(`/tables/${tableName}`);
      setTableColumns(response.data.columns || []);
    } catch (error) {
      console.error('Error fetching columns:', error);
    }
  };

  const vectorizeTable = async () => {
    if (!selectedTable || selectedColumns.length === 0) return;

    setLoading(true);
    try {
      await api.post(`/vectorize/${selectedTable}`, selectedColumns);
      setVectorizedTables([...vectorizedTables, selectedTable]);
    } catch (error) {
      console.error('Error vectorizing table:', error);
    }
    setLoading(false);
  };

  const askQuestion = async () => {
    if (!question.trim()) return;

    setLoading(true);
    setAnswer(null);

    try {
      if (mode === 'ai') {
        // Use AI to generate SQL
        const sqlResponse = await api.post('/ai/sql', { question });
        const sqlQuery = sqlResponse.data.sql;

        // Execute the SQL
        const queryResponse = await api.post('/query', { query: sqlQuery });

        // Get explanation
        let explanation = '';
        try {
          const explainResponse = await api.post('/ai/explain', {
            sql: sqlQuery,
            question,
            results: queryResponse.data.data || []
          });
          explanation = explainResponse.data.explanation;
        } catch (e) {
          explanation = 'Query executed successfully.';
        }

        setAnswer({
          answer: explanation,
          sql: sqlQuery,
          source_data: queryResponse.data.data?.slice(0, 10) || [],
          model_used: sqlResponse.data.model_used
        });
      } else {
        // Use semantic search
        const response = await api.post('/ask', {
          question,
          table_name: selectedTable || null
        });
        setAnswer(response.data);
      }
    } catch (error) {
      console.error('Error asking question:', error);
      setAnswer({ answer: 'Error: ' + (error.response?.data?.detail || error.message) });
    }
    setLoading(false);
  };

  return (
    <div className="ask-data">
      <div className="mode-toggle">
        <button
          className={mode === 'semantic' ? 'active' : ''}
          onClick={() => setMode('semantic')}
        >
          <Search size={16} /> Semantic Search
        </button>
        <button
          className={mode === 'ai' ? 'active' : ''}
          onClick={() => setMode('ai')}
        >
          <MessageSquare size={16} /> AI Query
        </button>
      </div>

      {ollamaStatus && (
        <div className={`ollama-status ${ollamaStatus.status}`}>
          {ollamaStatus.status === 'connected' ? (
            <>
              <CheckCircle size={16} />
              <span>Ollama connected. Models: {ollamaStatus.models.join(', ') || 'None installed'}</span>
            </>
          ) : (
            <>
              <AlertCircle size={16} />
              <span>Ollama not connected - AI features may be limited</span>
            </>
          )}
        </div>
      )}

      {mode === 'semantic' && (
        <div className="setup-section">
          <h3>Setup Semantic Search</h3>
          <p>First, select a table and columns to vectorize for natural language search.</p>

          <div className="vectorize-form">
            <select
              value={selectedTable}
              onChange={(e) => {
                setSelectedTable(e.target.value);
                fetchTableColumns(e.target.value);
              }}
            >
              <option value="">Select a table...</option>
              {tables.map(table => (
                <option key={table} value={table}>{table}</option>
              ))}
            </select>

            {tableColumns.length > 0 && (
              <div className="column-selector">
                <p>Select text columns to vectorize:</p>
                {tableColumns.map(col => (
                  <label key={col.name}>
                    <input
                      type="checkbox"
                      checked={selectedColumns.includes(col.name)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedColumns([...selectedColumns, col.name]);
                        } else {
                          setSelectedColumns(selectedColumns.filter(c => c !== col.name));
                        }
                      }}
                    />
                    {col.name} ({col.type})
                  </label>
                ))}
              </div>
            )}

            <button
              onClick={vectorizeTable}
              disabled={!selectedTable || selectedColumns.length === 0 || loading}
            >
              {loading ? 'Vectorizing...' : 'Vectorize Table'}
            </button>
          </div>

          {vectorizedTables.length > 0 && (
            <div className="vectorized-list">
              <strong>Vectorized tables:</strong>
              {vectorizedTables.map(t => (
                <span key={t} className="vectorized-badge">{t}</span>
              ))}
            </div>
          )}
        </div>
      )}

      <div className="ask-section">
        <h3>Ask a Question</h3>
        <div className="question-input">
          <input
            type="text"
            placeholder={mode === 'ai'
              ? "e.g., What's the average closing price for AAPL?"
              : "e.g., What is the parent company of Microsoft?"}
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && askQuestion()}
          />
          <button onClick={askQuestion} disabled={loading}>
            <Search size={16} />
            {loading ? 'Searching...' : 'Ask'}
          </button>
        </div>

        {answer && (
          <div className="answer-section">
            <div className="answer-text">
              <MessageSquare size={20} />
              <p>{answer.answer}</p>
            </div>

            {answer.sql && (
              <div className="sql-used">
                <h4>SQL Generated:</h4>
                <pre>{answer.sql}</pre>
                {answer.model_used && <small>Model: {answer.model_used}</small>}
              </div>
            )}

            {answer.source_data && answer.source_data.length > 0 && (
              <div className="source-data">
                <h4>Source Data</h4>
                {answer.source_data.map((item, i) => (
                  <div key={i} className="source-item">
                    {item.similarity && (
                      <span className="similarity">
                        {(item.similarity * 100).toFixed(1)}% match
                      </span>
                    )}
                    <pre>{JSON.stringify(item.data || item, null, 2)}</pre>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// Main App component
function App() {
  const [activeTab, setActiveTab] = useState('data');

  return (
    <div className="app">
      <header className="app-header">
        <div className="logo">
          <Database size={24} />
          <h1>Open Data Platform</h1>
        </div>
        <div className="header-actions">
          <a
            href="http://localhost:8081"
            target="_blank"
            rel="noopener noreferrer"
            className="airflow-link"
          >
            Open Airflow UI
          </a>
        </div>
      </header>

      <TabNav activeTab={activeTab} setActiveTab={setActiveTab} />

      <main className="app-main">
        {activeTab === 'data' && <DataExplorer />}
        {activeTab === 'sql' && <SQLEditor />}
        {activeTab === 'pipelines' && <PipelineCreator />}
        {activeTab === 'transforms' && <Transforms />}
        {activeTab === 'jobs' && <Jobs />}
        {activeTab === 'ask' && <AskData />}
      </main>
    </div>
  );
}

export default App;
