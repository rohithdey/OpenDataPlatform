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
  CheckCircle
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
    { id: 'pipelines', label: 'Create Pipeline', icon: Plus },
    { id: 'transforms', label: 'Transforms', icon: BarChart3 },
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
  const [limit, setLimit] = useState(1000);
  const [offset, setOffset] = useState(0);

  const executeQuery = async (newOffset = 0) => {
    setLoading(true);
    setError(null);
    try {
      const response = await api.post('/query', {
        query,
        limit,
        offset: newOffset
      });
      setResults(response.data);
      setOffset(newOffset);
    } catch (err) {
      setError(err.response?.data?.detail || 'Query execution failed');
      setResults(null);
    }
    setLoading(false);
  };

  const nextPage = () => {
    executeQuery(offset + limit);
  };

  const prevPage = () => {
    executeQuery(Math.max(0, offset - limit));
  };

  return (
    <div className="sql-editor">
      <div className="editor-section">
        <div className="editor-header">
          <h3>SQL Query</h3>
          <button onClick={() => executeQuery(0)} disabled={loading} className="run-button">
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
              executeQuery(0);
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
              {results.total_rows !== undefined ? (
                <span>
                  Showing {offset + 1}-{offset + results.row_count} of {results.total_rows.toLocaleString()} total rows
                </span>
              ) : results.row_count !== undefined && (
                <span>{results.row_count} rows returned</span>
              )}
            </div>

            {results.is_truncated && (
              <div className="warning-message" style={{
                padding: '12px',
                backgroundColor: 'rgba(251, 191, 36, 0.15)',
                border: '1px solid rgba(251, 191, 36, 0.4)',
                borderRadius: '4px',
                marginBottom: '12px',
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                color: '#fbbf24'
              }}>
                <AlertCircle size={16} />
                <span>
                  Results limited to {limit.toLocaleString()} rows for performance.
                  {results.total_rows && ` Total: ${results.total_rows.toLocaleString()} rows.`}
                  Use pagination controls below to view more data.
                </span>
              </div>
            )}

            {results.data ? (
              <>
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

                {results.total_rows > limit && (
                  <div className="pagination-controls" style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    padding: '12px',
                    borderTop: '1px solid rgba(255, 255, 255, 0.1)',
                    marginTop: '12px',
                    backgroundColor: 'rgba(0, 0, 0, 0.2)'
                  }}>
                    <button
                      onClick={prevPage}
                      disabled={offset === 0}
                      style={{
                        padding: '8px 16px',
                        opacity: offset === 0 ? 0.5 : 1
                      }}
                    >
                      Previous
                    </button>
                    <span style={{ fontSize: '14px' }}>
                      Page {Math.floor(offset / limit) + 1} of {Math.ceil(results.total_rows / limit)}
                    </span>
                    <button
                      onClick={nextPage}
                      disabled={offset + results.row_count >= results.total_rows}
                      style={{
                        padding: '8px 16px',
                        opacity: (offset + results.row_count >= results.total_rows) ? 0.5 : 1
                      }}
                    >
                      Next
                    </button>
                  </div>
                )}
              </>
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

// Ask Data component (Natural Language to SQL)
function AskData() {
  const [question, setQuestion] = useState('');
  const [answer, setAnswer] = useState(null);
  const [loading, setLoading] = useState(false);
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');

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

  const askQuestion = async () => {
    if (!question.trim()) return;

    setLoading(true);
    setAnswer(null);
    try {
      const response = await api.post('/ask', {
        question,
        table_name: selectedTable || null
      });
      setAnswer(response.data);
    } catch (error) {
      console.error('Error asking question:', error);
      setAnswer({
        answer: 'Error processing question. Please try again.',
        error: error.response?.data?.detail || error.message
      });
    }
    setLoading(false);
  };

  return (
    <div className="ask-data">
      <div className="ask-header" style={{ marginBottom: '24px' }}>
        <h2>Ask Your Data</h2>
        <p style={{ color: '#a0aec0', marginTop: '8px' }}>
          Ask questions in plain English and get instant answers powered by AI and SQL.
          No setup required - just type your question!
        </p>
      </div>

      <div className="ask-section">
        <div className="question-form" style={{ marginBottom: '20px' }}>
          <div style={{ marginBottom: '12px' }}>
            <label style={{ display: 'block', marginBottom: '6px', fontWeight: '500' }}>
              Optional: Limit to specific table
            </label>
            <select
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
              style={{ width: '100%', padding: '8px', borderRadius: '4px', border: '1px solid #ddd' }}
            >
              <option value="">All tables</option>
              {tables.map(table => (
                <option key={table} value={table}>{table}</option>
              ))}
            </select>
          </div>

          <div className="question-input" style={{ display: 'flex', gap: '8px' }}>
            <input
              type="text"
              placeholder="e.g., What are the top 5 customers by revenue? How many orders were placed last month?"
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && !loading && askQuestion()}
              style={{
                flex: 1,
                padding: '12px',
                fontSize: '16px',
                borderRadius: '4px',
                border: '1px solid #ddd'
              }}
            />
            <button
              onClick={askQuestion}
              disabled={loading || !question.trim()}
              style={{
                padding: '12px 24px',
                fontSize: '16px',
                display: 'flex',
                alignItems: 'center',
                gap: '8px'
              }}
            >
              <Search size={16} />
              {loading ? 'Thinking...' : 'Ask'}
            </button>
          </div>
        </div>

        {answer && (
          <div className="answer-section">
            <div className="answer-card" style={{
              padding: '20px',
              backgroundColor: 'rgba(59, 130, 246, 0.1)',
              border: '1px solid rgba(59, 130, 246, 0.3)',
              borderRadius: '8px',
              marginBottom: '16px'
            }}>
              <div style={{ display: 'flex', alignItems: 'start', gap: '12px' }}>
                <MessageSquare size={24} style={{ flexShrink: 0, marginTop: '2px', color: '#3b82f6' }} />
                <div style={{ flex: 1 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                    <h3 style={{ color: '#3b82f6', margin: 0 }}>Answer:</h3>
                    {answer.llm_source && (
                      <span style={{
                        fontSize: '12px',
                        padding: '4px 8px',
                        backgroundColor: answer.llm_source.includes('Ollama') ? 'rgba(34, 197, 94, 0.2)' : 'rgba(168, 85, 247, 0.2)',
                        border: `1px solid ${answer.llm_source.includes('Ollama') ? 'rgba(34, 197, 94, 0.4)' : 'rgba(168, 85, 247, 0.4)'}`,
                        borderRadius: '4px',
                        color: answer.llm_source.includes('Ollama') ? '#22c55e' : '#a855f7'
                      }}>
                        🚀 {answer.llm_source}
                      </span>
                    )}
                  </div>
                  <p style={{ fontSize: '16px', lineHeight: '1.6' }}>{answer.answer}</p>
                </div>
              </div>
            </div>

            {answer.sql_query && (
              <div className="sql-query-card" style={{
                padding: '16px',
                backgroundColor: 'rgba(0, 0, 0, 0.2)',
                border: '1px solid rgba(255, 255, 255, 0.1)',
                borderRadius: '8px',
                marginBottom: '16px'
              }}>
                <h4 style={{ marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <FileText size={18} />
                  Generated SQL Query:
                </h4>
                <pre style={{
                  backgroundColor: 'rgba(0, 0, 0, 0.3)',
                  padding: '12px',
                  borderRadius: '4px',
                  overflowX: 'auto',
                  fontSize: '14px',
                  fontFamily: 'monospace',
                  color: '#a0aec0'
                }}>
                  {answer.sql_query}
                </pre>
              </div>
            )}

            {answer.result_data && answer.result_data.length > 0 && (
              <div className="result-data-card" style={{
                padding: '16px',
                backgroundColor: 'rgba(0, 0, 0, 0.2)',
                border: '1px solid rgba(255, 255, 255, 0.1)',
                borderRadius: '8px'
              }}>
                <h4 style={{ marginBottom: '12px' }}>
                  Query Results {answer.row_count && `(${answer.row_count} rows)`}:
                </h4>
                <div style={{ overflowX: 'auto' }}>
                  <table className="data-table">
                    <thead>
                      <tr>
                        {Object.keys(answer.result_data[0]).map(key => (
                          <th key={key}>{key}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {answer.result_data.map((row, i) => (
                        <tr key={i}>
                          {Object.keys(row).map(key => (
                            <td key={key}>{String(row[key] ?? '')}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {answer.error && (
              <div className="error-message" style={{
                padding: '12px',
                backgroundColor: 'rgba(239, 68, 68, 0.1)',
                border: '1px solid rgba(239, 68, 68, 0.3)',
                borderRadius: '4px',
                marginTop: '12px'
              }}>
                <AlertCircle size={16} />
                <span>{answer.error}</span>
              </div>
            )}
          </div>
        )}

        {!answer && !loading && (
          <div style={{
            padding: '40px',
            textAlign: 'center',
            color: '#a0aec0'
          }}>
            <MessageSquare size={48} style={{ opacity: 0.3, marginBottom: '16px' }} />
            <p>Ask a question about your data to get started</p>
            <p style={{ fontSize: '14px', marginTop: '8px', color: '#718096' }}>
              Example: "Show me the top 10 customers", "What's the average order value?"
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

// Pipeline Creator component
function PipelineCreator() {
  const [sourceType, setSourceType] = useState('yahoo_finance');
  const [symbols, setSymbols] = useState('AAPL,TSLA,MSFT');
  const [period, setPeriod] = useState('1y');
  const [dagId, setDagId] = useState('yahoo_finance_daily');
  const [tableName, setTableName] = useState('stock_prices');
  const [schedule, setSchedule] = useState('0 16 * * 1-5');
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState(null);
  const [fetchLoading, setFetchLoading] = useState(false);

  // Cron builder state
  const [scheduleMode, setScheduleMode] = useState('natural'); // 'natural', 'visual', 'manual'
  const [naturalLanguage, setNaturalLanguage] = useState('weekdays at 4pm');
  const [cronDescription, setCronDescription] = useState('Weekdays at 4:00 PM');
  const [visualFrequency, setVisualFrequency] = useState('weekdays');
  const [visualHour, setVisualHour] = useState('16');
  const [visualMinute, setVisualMinute] = useState('0');
  const [visualDays, setVisualDays] = useState('1-5');

  // Initialize cron description on mount
  useEffect(() => {
    updateCronDescription(schedule);
  }, []);

  const createDAG = async () => {
    setLoading(true);
    setMessage(null);
    try {
      const symbolList = symbols.split(',').map(s => s.trim()).filter(s => s);

      const response = await api.post('/dags/yahoo-finance/create', {
        dag_id: dagId,
        description: `Yahoo Finance data ingestion for ${symbolList.join(', ')}`,
        symbols: symbolList,
        period: period,
        interval: '1d',
        schedule: schedule,
        target_table: tableName
      });

      setMessage({ type: 'success', text: response.data.message });
    } catch (error) {
      setMessage({ type: 'error', text: error.response?.data?.detail || error.message });
    }
    setLoading(false);
  };

  const fetchNow = async () => {
    setFetchLoading(true);
    setMessage(null);
    try {
      const symbolList = symbols.split(',').map(s => s.trim()).filter(s => s);

      const response = await api.post('/dags/yahoo-finance/fetch-now', {
        symbols: symbolList,
        period: period,
        table_name: tableName
      });

      setMessage({
        type: 'success',
        text: `✓ Fetched ${response.data.rows_loaded} rows for ${symbolList.join(', ')}`
      });
    } catch (error) {
      setMessage({ type: 'error', text: error.response?.data?.detail || error.message });
    }
    setFetchLoading(false);
  };

  const parseNaturalLanguage = async () => {
    try {
      const response = await api.post('/cron/parse', { text: naturalLanguage });
      if (response.data.error) {
        setMessage({ type: 'error', text: response.data.error });
      } else {
        setSchedule(response.data.cron);
        setCronDescription(response.data.description);
        setMessage(null);
      }
    } catch (error) {
      setMessage({ type: 'error', text: 'Failed to parse natural language' });
    }
  };

  const buildVisualCron = () => {
    let cron = '';
    const minute = visualMinute || '0';
    const hour = visualHour || '0';

    if (visualFrequency === 'minutes') {
      cron = `*/${visualMinute || 30} * * * *`;
    } else if (visualFrequency === 'hourly') {
      cron = `0 * * * *`;
    } else if (visualFrequency === 'daily') {
      cron = `${minute} ${hour} * * *`;
    } else if (visualFrequency === 'weekdays') {
      cron = `${minute} ${hour} * * 1-5`;
    } else if (visualFrequency === 'weekends') {
      cron = `${minute} ${hour} * * 0,6`;
    } else if (visualFrequency === 'weekly') {
      cron = `${minute} ${hour} * * ${visualDays}`;
    } else if (visualFrequency === 'monthly') {
      cron = `${minute} ${hour} ${visualDays} * *`;
    }

    setSchedule(cron);
    updateCronDescription(cron);
  };

  const updateCronDescription = (cronExpr) => {
    // Simple cron description (you could also call the backend for this)
    const parts = cronExpr.split(' ');
    if (parts.length === 5) {
      const [min, hr, day, month, weekday] = parts;

      if (cronExpr.includes('*/')) {
        setCronDescription(`Every ${min.replace('*/', '')} minutes`);
      } else if (hr === '*' && min === '0') {
        setCronDescription('Every hour');
      } else if (weekday === '1-5') {
        const h = parseInt(hr);
        const meridiem = h >= 12 ? 'PM' : 'AM';
        const displayHour = h > 12 ? h - 12 : (h === 0 ? 12 : h);
        setCronDescription(`Weekdays at ${displayHour}:${min.padStart(2, '0')} ${meridiem}`);
      } else if (weekday === '0,6') {
        const h = parseInt(hr);
        const meridiem = h >= 12 ? 'PM' : 'AM';
        const displayHour = h > 12 ? h - 12 : (h === 0 ? 12 : h);
        setCronDescription(`Weekends at ${displayHour}:${min.padStart(2, '0')} ${meridiem}`);
      } else if (weekday !== '*') {
        const h = parseInt(hr);
        const meridiem = h >= 12 ? 'PM' : 'AM';
        const displayHour = h > 12 ? h - 12 : (h === 0 ? 12 : h);
        const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
        const dayName = dayNames[parseInt(weekday)] || `Day ${weekday}`;
        setCronDescription(`Weekly on ${dayName} at ${displayHour}:${min.padStart(2, '0')} ${meridiem}`);
      } else if (day !== '*') {
        const h = parseInt(hr);
        const meridiem = h >= 12 ? 'PM' : 'AM';
        const displayHour = h > 12 ? h - 12 : (h === 0 ? 12 : h);
        setCronDescription(`Monthly on day ${day} at ${displayHour}:${min.padStart(2, '0')} ${meridiem}`);
      } else {
        const h = parseInt(hr);
        const meridiem = h >= 12 ? 'PM' : 'AM';
        const displayHour = h > 12 ? h - 12 : (h === 0 ? 12 : h);
        setCronDescription(`Daily at ${displayHour}:${min.padStart(2, '0')} ${meridiem}`);
      }
    }
  };

  return (
    <div className="pipeline-creator" style={{ padding: '24px', maxWidth: '800px', margin: '0 auto' }}>
      <h2 style={{ marginBottom: '24px', color: '#f7fafc' }}>Create Data Pipeline</h2>

      <div style={{ marginBottom: '24px', padding: '16px', backgroundColor: 'rgba(66, 153, 225, 0.1)', borderRadius: '8px', border: '1px solid rgba(66, 153, 225, 0.2)' }}>
        <p style={{ margin: 0, fontSize: '14px', color: '#90cdf4' }}>
          💡 This creates an Airflow DAG that automatically fetches stock data on schedule
        </p>
      </div>

      <div style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)', padding: '24px', borderRadius: '8px', marginBottom: '24px' }}>
        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0' }}>
            Data Source
          </label>
          <select
            value={sourceType}
            onChange={(e) => setSourceType(e.target.value)}
            style={{
              width: '100%',
              padding: '10px',
              backgroundColor: '#2d3748',
              border: '1px solid #4a5568',
              borderRadius: '4px',
              color: '#e2e8f0'
            }}
          >
            <option value="yahoo_finance">Yahoo Finance (Stock Prices)</option>
          </select>
        </div>

        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0' }}>
            Stock Symbols (comma-separated)
          </label>
          <input
            type="text"
            value={symbols}
            onChange={(e) => setSymbols(e.target.value)}
            placeholder="AAPL,TSLA,MSFT,GOOGL"
            style={{
              width: '100%',
              padding: '10px',
              backgroundColor: '#2d3748',
              border: '1px solid #4a5568',
              borderRadius: '4px',
              color: '#e2e8f0'
            }}
          />
        </div>

        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0' }}>
            Historical Period
          </label>
          <select
            value={period}
            onChange={(e) => setPeriod(e.target.value)}
            style={{
              width: '100%',
              padding: '10px',
              backgroundColor: '#2d3748',
              border: '1px solid #4a5568',
              borderRadius: '4px',
              color: '#e2e8f0'
            }}
          >
            <option value="1mo">1 Month</option>
            <option value="3mo">3 Months</option>
            <option value="6mo">6 Months</option>
            <option value="1y">1 Year</option>
            <option value="2y">2 Years</option>
            <option value="5y">5 Years</option>
          </select>
        </div>

        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0' }}>
            DAG ID
          </label>
          <input
            type="text"
            value={dagId}
            onChange={(e) => setDagId(e.target.value)}
            placeholder="my_stock_pipeline"
            style={{
              width: '100%',
              padding: '10px',
              backgroundColor: '#2d3748',
              border: '1px solid #4a5568',
              borderRadius: '4px',
              color: '#e2e8f0'
            }}
          />
        </div>

        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0' }}>
            Target Table Name
          </label>
          <input
            type="text"
            value={tableName}
            onChange={(e) => setTableName(e.target.value)}
            placeholder="stock_prices"
            style={{
              width: '100%',
              padding: '10px',
              backgroundColor: '#2d3748',
              border: '1px solid #4a5568',
              borderRadius: '4px',
              color: '#e2e8f0'
            }}
          />
        </div>

        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0' }}>
            Schedule
          </label>

          {/* Mode Toggle */}
          <div style={{ display: 'flex', gap: '8px', marginBottom: '12px' }}>
            <button
              onClick={() => setScheduleMode('natural')}
              style={{
                padding: '8px 16px',
                backgroundColor: scheduleMode === 'natural' ? 'rgba(66, 153, 225, 0.3)' : '#2d3748',
                color: scheduleMode === 'natural' ? '#90cdf4' : '#a0aec0',
                border: scheduleMode === 'natural' ? '1px solid rgba(66, 153, 225, 0.5)' : '1px solid #4a5568',
                borderRadius: '4px',
                cursor: 'pointer',
                fontSize: '13px',
                fontWeight: '500'
              }}
            >
              💬 Natural Language
            </button>
            <button
              onClick={() => setScheduleMode('visual')}
              style={{
                padding: '8px 16px',
                backgroundColor: scheduleMode === 'visual' ? 'rgba(66, 153, 225, 0.3)' : '#2d3748',
                color: scheduleMode === 'visual' ? '#90cdf4' : '#a0aec0',
                border: scheduleMode === 'visual' ? '1px solid rgba(66, 153, 225, 0.5)' : '1px solid #4a5568',
                borderRadius: '4px',
                cursor: 'pointer',
                fontSize: '13px',
                fontWeight: '500'
              }}
            >
              🎨 Visual Builder
            </button>
            <button
              onClick={() => setScheduleMode('manual')}
              style={{
                padding: '8px 16px',
                backgroundColor: scheduleMode === 'manual' ? 'rgba(66, 153, 225, 0.3)' : '#2d3748',
                color: scheduleMode === 'manual' ? '#90cdf4' : '#a0aec0',
                border: scheduleMode === 'manual' ? '1px solid rgba(66, 153, 225, 0.5)' : '1px solid #4a5568',
                borderRadius: '4px',
                cursor: 'pointer',
                fontSize: '13px',
                fontWeight: '500'
              }}
            >
              ⚙️ Manual
            </button>
          </div>

          {/* Natural Language Mode */}
          {scheduleMode === 'natural' && (
            <div>
              <div style={{ display: 'flex', gap: '8px', marginBottom: '8px' }}>
                <input
                  type="text"
                  value={naturalLanguage}
                  onChange={(e) => setNaturalLanguage(e.target.value)}
                  onKeyPress={(e) => e.key === 'Enter' && parseNaturalLanguage()}
                  placeholder="e.g., daily at 5pm, every 30 minutes, weekdays at 9am"
                  style={{
                    flex: 1,
                    padding: '10px',
                    backgroundColor: '#2d3748',
                    border: '1px solid #4a5568',
                    borderRadius: '4px',
                    color: '#e2e8f0'
                  }}
                />
                <button
                  onClick={parseNaturalLanguage}
                  style={{
                    padding: '10px 20px',
                    backgroundColor: 'rgba(72, 187, 120, 0.2)',
                    color: '#68d391',
                    border: '1px solid rgba(72, 187, 120, 0.3)',
                    borderRadius: '4px',
                    cursor: 'pointer',
                    fontWeight: '500'
                  }}
                >
                  Parse
                </button>
              </div>
              <p style={{ marginTop: '4px', fontSize: '12px', color: '#a0aec0' }}>
                Examples: "daily at 5pm", "every 30 minutes", "weekdays at 9am", "monthly on day 15"
              </p>
            </div>
          )}

          {/* Visual Builder Mode */}
          {scheduleMode === 'visual' && (
            <div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px', marginBottom: '8px' }}>
                <div>
                  <label style={{ display: 'block', marginBottom: '4px', fontSize: '13px', color: '#a0aec0' }}>
                    Frequency
                  </label>
                  <select
                    value={visualFrequency}
                    onChange={(e) => { setVisualFrequency(e.target.value); buildVisualCron(); }}
                    style={{
                      width: '100%',
                      padding: '8px',
                      backgroundColor: '#2d3748',
                      border: '1px solid #4a5568',
                      borderRadius: '4px',
                      color: '#e2e8f0',
                      fontSize: '13px'
                    }}
                  >
                    <option value="minutes">Every N Minutes</option>
                    <option value="hourly">Hourly</option>
                    <option value="daily">Daily</option>
                    <option value="weekdays">Weekdays</option>
                    <option value="weekends">Weekends</option>
                    <option value="weekly">Weekly</option>
                    <option value="monthly">Monthly</option>
                  </select>
                </div>

                {visualFrequency === 'minutes' && (
                  <div>
                    <label style={{ display: 'block', marginBottom: '4px', fontSize: '13px', color: '#a0aec0' }}>
                      Every N Minutes
                    </label>
                    <input
                      type="number"
                      value={visualMinute}
                      onChange={(e) => { setVisualMinute(e.target.value); buildVisualCron(); }}
                      min="1"
                      max="59"
                      style={{
                        width: '100%',
                        padding: '8px',
                        backgroundColor: '#2d3748',
                        border: '1px solid #4a5568',
                        borderRadius: '4px',
                        color: '#e2e8f0',
                        fontSize: '13px'
                      }}
                    />
                  </div>
                )}

                {(visualFrequency === 'daily' || visualFrequency === 'weekdays' || visualFrequency === 'weekends' || visualFrequency === 'weekly') && (
                  <>
                    <div>
                      <label style={{ display: 'block', marginBottom: '4px', fontSize: '13px', color: '#a0aec0' }}>
                        Hour (24h)
                      </label>
                      <input
                        type="number"
                        value={visualHour}
                        onChange={(e) => { setVisualHour(e.target.value); buildVisualCron(); }}
                        min="0"
                        max="23"
                        style={{
                          width: '100%',
                          padding: '8px',
                          backgroundColor: '#2d3748',
                          border: '1px solid #4a5568',
                          borderRadius: '4px',
                          color: '#e2e8f0',
                          fontSize: '13px'
                        }}
                      />
                    </div>
                    <div>
                      <label style={{ display: 'block', marginBottom: '4px', fontSize: '13px', color: '#a0aec0' }}>
                        Minute
                      </label>
                      <input
                        type="number"
                        value={visualMinute}
                        onChange={(e) => { setVisualMinute(e.target.value); buildVisualCron(); }}
                        min="0"
                        max="59"
                        style={{
                          width: '100%',
                          padding: '8px',
                          backgroundColor: '#2d3748',
                          border: '1px solid #4a5568',
                          borderRadius: '4px',
                          color: '#e2e8f0',
                          fontSize: '13px'
                        }}
                      />
                    </div>
                  </>
                )}

                {visualFrequency === 'weekly' && (
                  <div style={{ gridColumn: '1 / -1' }}>
                    <label style={{ display: 'block', marginBottom: '4px', fontSize: '13px', color: '#a0aec0' }}>
                      Day of Week
                    </label>
                    <select
                      value={visualDays}
                      onChange={(e) => { setVisualDays(e.target.value); buildVisualCron(); }}
                      style={{
                        width: '100%',
                        padding: '8px',
                        backgroundColor: '#2d3748',
                        border: '1px solid #4a5568',
                        borderRadius: '4px',
                        color: '#e2e8f0',
                        fontSize: '13px'
                      }}
                    >
                      <option value="1">Monday</option>
                      <option value="2">Tuesday</option>
                      <option value="3">Wednesday</option>
                      <option value="4">Thursday</option>
                      <option value="5">Friday</option>
                      <option value="6">Saturday</option>
                      <option value="0">Sunday</option>
                    </select>
                  </div>
                )}

                {visualFrequency === 'monthly' && (
                  <div>
                    <label style={{ display: 'block', marginBottom: '4px', fontSize: '13px', color: '#a0aec0' }}>
                      Day of Month
                    </label>
                    <input
                      type="number"
                      value={visualDays}
                      onChange={(e) => { setVisualDays(e.target.value); buildVisualCron(); }}
                      min="1"
                      max="31"
                      style={{
                        width: '100%',
                        padding: '8px',
                        backgroundColor: '#2d3748',
                        border: '1px solid #4a5568',
                        borderRadius: '4px',
                        color: '#e2e8f0',
                        fontSize: '13px'
                      }}
                    />
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Manual Mode */}
          {scheduleMode === 'manual' && (
            <div>
              <input
                type="text"
                value={schedule}
                onChange={(e) => { setSchedule(e.target.value); updateCronDescription(e.target.value); }}
                placeholder="0 16 * * 1-5"
                style={{
                  width: '100%',
                  padding: '10px',
                  backgroundColor: '#2d3748',
                  border: '1px solid #4a5568',
                  borderRadius: '4px',
                  color: '#e2e8f0',
                  fontFamily: 'monospace'
                }}
              />
              <p style={{ marginTop: '4px', fontSize: '12px', color: '#a0aec0' }}>
                Example: "0 16 * * 1-5" = 4 PM weekdays (after market close)
              </p>
            </div>
          )}

          {/* Current Schedule Display */}
          {schedule && (
            <div style={{
              marginTop: '12px',
              padding: '10px',
              backgroundColor: 'rgba(66, 153, 225, 0.1)',
              border: '1px solid rgba(66, 153, 225, 0.3)',
              borderRadius: '4px'
            }}>
              <div style={{ fontSize: '12px', color: '#a0aec0', marginBottom: '4px' }}>Current Schedule:</div>
              <div style={{ fontFamily: 'monospace', color: '#90cdf4', fontSize: '13px', marginBottom: '4px' }}>
                {schedule}
              </div>
              {cronDescription && (
                <div style={{ fontSize: '12px', color: '#68d391' }}>
                  ✓ {cronDescription}
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      <div style={{ display: 'flex', gap: '12px' }}>
        <button
          onClick={fetchNow}
          disabled={fetchLoading}
          style={{
            flex: 1,
            padding: '12px 24px',
            backgroundColor: fetchLoading ? '#4a5568' : 'rgba(72, 187, 120, 0.2)',
            color: '#68d391',
            border: '1px solid rgba(72, 187, 120, 0.3)',
            borderRadius: '4px',
            cursor: fetchLoading ? 'not-allowed' : 'pointer',
            fontWeight: '500'
          }}
        >
          {fetchLoading ? 'Fetching...' : '⚡ Fetch Data Now (Test)'}
        </button>

        <button
          onClick={createDAG}
          disabled={loading}
          style={{
            flex: 1,
            padding: '12px 24px',
            backgroundColor: loading ? '#4a5568' : 'rgba(66, 153, 225, 0.2)',
            color: '#90cdf4',
            border: '1px solid rgba(66, 153, 225, 0.3)',
            borderRadius: '4px',
            cursor: loading ? 'not-allowed' : 'pointer',
            fontWeight: '500'
          }}
        >
          {loading ? 'Creating...' : '📅 Create Scheduled DAG'}
        </button>
      </div>

      {message && (
        <div style={{
          marginTop: '20px',
          padding: '12px',
          backgroundColor: message.type === 'success' ? 'rgba(72, 187, 120, 0.1)' : 'rgba(245, 101, 101, 0.1)',
          border: `1px solid ${message.type === 'success' ? 'rgba(72, 187, 120, 0.3)' : 'rgba(245, 101, 101, 0.3)'}`,
          borderRadius: '4px',
          color: message.type === 'success' ? '#68d391' : '#fc8181'
        }}>
          {message.text}
        </div>
      )}
    </div>
  );
}

// Transforms component
function Transforms() {
  const [templates, setTemplates] = useState([]);
  const [selectedTemplate, setSelectedTemplate] = useState(null);
  const [modelName, setModelName] = useState('');
  const [sourceTable, setSourceTable] = useState('stock_prices');
  const [dateColumn, setDateColumn] = useState('date');
  const [valueColumn, setValueColumn] = useState('close');
  const [groupByColumn, setGroupByColumn] = useState('symbol');
  const [windows, setWindows] = useState('7,30,90');
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState(null);

  useEffect(() => {
    fetchTemplates();
  }, []);

  const fetchTemplates = async () => {
    try {
      const response = await api.get('/dbt/templates');
      setTemplates(response.data.templates || []);
    } catch (error) {
      console.error('Error fetching templates:', error);
    }
  };

  const createTransform = async () => {
    if (!selectedTemplate || !modelName) {
      setMessage({ type: 'error', text: 'Please select a template and enter a model name' });
      return;
    }

    setLoading(true);
    setMessage(null);

    try {
      const parameters = {
        date_column: dateColumn,
        value_column: valueColumn,
        group_by_column: groupByColumn
      };

      if (selectedTemplate === 'moving_averages') {
        parameters.windows = windows.split(',').map(w => parseInt(w.trim()));
      }

      const response = await api.post('/dbt/create-from-template', {
        template_id: selectedTemplate,
        model_name: modelName,
        source_table: sourceTable,
        parameters: parameters
      });

      setMessage({ type: 'success', text: response.data.message });
      setModelName('');
    } catch (error) {
      setMessage({ type: 'error', text: error.response?.data?.detail || error.message });
    }
    setLoading(false);
  };

  return (
    <div className="transforms" style={{ padding: '24px', maxWidth: '800px', margin: '0 auto' }}>
      <h2 style={{ marginBottom: '24px', color: '#f7fafc' }}>Create Data Transformation</h2>

      <div style={{ marginBottom: '24px', padding: '16px', backgroundColor: 'rgba(159, 122, 234, 0.1)', borderRadius: '8px', border: '1px solid rgba(159, 122, 234, 0.2)' }}>
        <p style={{ margin: 0, fontSize: '14px', color: '#c4b5fd' }}>
          🔄 Transforms use DBT to calculate metrics like daily returns, moving averages, and volatility
        </p>
      </div>

      <div style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)', padding: '24px', borderRadius: '8px', marginBottom: '24px' }}>
        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0' }}>
            Transformation Template
          </label>
          <select
            value={selectedTemplate || ''}
            onChange={(e) => {
              setSelectedTemplate(e.target.value);
              setModelName(e.target.value ? `${e.target.value}_model` : '');
            }}
            style={{
              width: '100%',
              padding: '10px',
              backgroundColor: '#2d3748',
              border: '1px solid #4a5568',
              borderRadius: '4px',
              color: '#e2e8f0'
            }}
          >
            <option value="">Select a template...</option>
            {templates.map(template => (
              <option key={template.id} value={template.id}>
                {template.name} - {template.description}
              </option>
            ))}
          </select>
        </div>

        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0' }}>
            Model Name
          </label>
          <input
            type="text"
            value={modelName}
            onChange={(e) => setModelName(e.target.value)}
            placeholder="my_transformation"
            style={{
              width: '100%',
              padding: '10px',
              backgroundColor: '#2d3748',
              border: '1px solid #4a5568',
              borderRadius: '4px',
              color: '#e2e8f0'
            }}
          />
        </div>

        <div style={{ marginBottom: '20px' }}>
          <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0' }}>
            Source Table
          </label>
          <input
            type="text"
            value={sourceTable}
            onChange={(e) => setSourceTable(e.target.value)}
            placeholder="stock_prices"
            style={{
              width: '100%',
              padding: '10px',
              backgroundColor: '#2d3748',
              border: '1px solid #4a5568',
              borderRadius: '4px',
              color: '#e2e8f0'
            }}
          />
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '12px', marginBottom: '20px' }}>
          <div>
            <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0', fontSize: '14px' }}>
              Date Column
            </label>
            <input
              type="text"
              value={dateColumn}
              onChange={(e) => setDateColumn(e.target.value)}
              style={{
                width: '100%',
                padding: '8px',
                backgroundColor: '#2d3748',
                border: '1px solid #4a5568',
                borderRadius: '4px',
                color: '#e2e8f0'
              }}
            />
          </div>
          <div>
            <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0', fontSize: '14px' }}>
              Value Column
            </label>
            <input
              type="text"
              value={valueColumn}
              onChange={(e) => setValueColumn(e.target.value)}
              style={{
                width: '100%',
                padding: '8px',
                backgroundColor: '#2d3748',
                border: '1px solid #4a5568',
                borderRadius: '4px',
                color: '#e2e8f0'
              }}
            />
          </div>
          <div>
            <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0', fontSize: '14px' }}>
              Group By Column
            </label>
            <input
              type="text"
              value={groupByColumn}
              onChange={(e) => setGroupByColumn(e.target.value)}
              style={{
                width: '100%',
                padding: '8px',
                backgroundColor: '#2d3748',
                border: '1px solid #4a5568',
                borderRadius: '4px',
                color: '#e2e8f0'
              }}
            />
          </div>
        </div>

        {selectedTemplate === 'moving_averages' && (
          <div style={{ marginBottom: '20px' }}>
            <label style={{ display: 'block', marginBottom: '8px', fontWeight: '500', color: '#e2e8f0' }}>
              Moving Average Windows (comma-separated days)
            </label>
            <input
              type="text"
              value={windows}
              onChange={(e) => setWindows(e.target.value)}
              placeholder="7,30,90"
              style={{
                width: '100%',
                padding: '10px',
                backgroundColor: '#2d3748',
                border: '1px solid #4a5568',
                borderRadius: '4px',
                color: '#e2e8f0'
              }}
            />
          </div>
        )}
      </div>

      <button
        onClick={createTransform}
        disabled={loading || !selectedTemplate || !modelName}
        style={{
          width: '100%',
          padding: '12px 24px',
          backgroundColor: (loading || !selectedTemplate || !modelName) ? '#4a5568' : 'rgba(159, 122, 234, 0.2)',
          color: '#c4b5fd',
          border: '1px solid rgba(159, 122, 234, 0.3)',
          borderRadius: '4px',
          cursor: (loading || !selectedTemplate || !modelName) ? 'not-allowed' : 'pointer',
          fontWeight: '500'
        }}
      >
        {loading ? 'Creating...' : '✨ Create Transformation'}
      </button>

      {message && (
        <div style={{
          marginTop: '20px',
          padding: '12px',
          backgroundColor: message.type === 'success' ? 'rgba(72, 187, 120, 0.1)' : 'rgba(245, 101, 101, 0.1)',
          border: `1px solid ${message.type === 'success' ? 'rgba(72, 187, 120, 0.3)' : 'rgba(245, 101, 101, 0.3)'}`,
          borderRadius: '4px',
          color: message.type === 'success' ? '#68d391' : '#fc8181'
        }}>
          {message.text}
        </div>
      )}
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
            href="http://localhost:8080" 
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
