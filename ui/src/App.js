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
                backgroundColor: '#fff3cd',
                border: '1px solid #ffc107',
                borderRadius: '4px',
                marginBottom: '12px',
                display: 'flex',
                alignItems: 'center',
                gap: '8px'
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
                    borderTop: '1px solid #ddd',
                    marginTop: '12px'
                  }}>
                    <button
                      onClick={prevPage}
                      disabled={offset === 0}
                      style={{ padding: '8px 16px' }}
                    >
                      Previous
                    </button>
                    <span>
                      Page {Math.floor(offset / limit) + 1} of {Math.ceil(results.total_rows / limit)}
                    </span>
                    <button
                      onClick={nextPage}
                      disabled={offset + results.row_count >= results.total_rows}
                      style={{ padding: '8px 16px' }}
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
        <p style={{ color: '#666', marginTop: '8px' }}>
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
              backgroundColor: '#f8f9fa',
              borderRadius: '8px',
              marginBottom: '16px'
            }}>
              <div style={{ display: 'flex', alignItems: 'start', gap: '12px' }}>
                <MessageSquare size={24} style={{ flexShrink: 0, marginTop: '2px' }} />
                <div style={{ flex: 1 }}>
                  <h3 style={{ marginBottom: '8px' }}>Answer:</h3>
                  <p style={{ fontSize: '16px', lineHeight: '1.6' }}>{answer.answer}</p>
                </div>
              </div>
            </div>

            {answer.sql_query && (
              <div className="sql-query-card" style={{
                padding: '16px',
                backgroundColor: '#fff',
                border: '1px solid #ddd',
                borderRadius: '8px',
                marginBottom: '16px'
              }}>
                <h4 style={{ marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <FileText size={18} />
                  Generated SQL Query:
                </h4>
                <pre style={{
                  backgroundColor: '#f5f5f5',
                  padding: '12px',
                  borderRadius: '4px',
                  overflowX: 'auto',
                  fontSize: '14px',
                  fontFamily: 'monospace'
                }}>
                  {answer.sql_query}
                </pre>
              </div>
            )}

            {answer.result_data && answer.result_data.length > 0 && (
              <div className="result-data-card" style={{
                padding: '16px',
                backgroundColor: '#fff',
                border: '1px solid #ddd',
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
                backgroundColor: '#fee',
                border: '1px solid #fcc',
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
            color: '#666'
          }}>
            <MessageSquare size={48} style={{ opacity: 0.3, marginBottom: '16px' }} />
            <p>Ask a question about your data to get started</p>
            <p style={{ fontSize: '14px', marginTop: '8px' }}>
              Example: "Show me the top 10 customers", "What's the average order value?"
            </p>
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
        {activeTab === 'jobs' && <Jobs />}
        {activeTab === 'ask' && <AskData />}
      </main>
    </div>
  );
}

export default App;
