import { useEffect, useState, useRef } from 'react'
import { useRouter } from 'next/router'
import React from 'react'; // Added missing import for React

const API_BASE = 'http://127.0.0.1:8000/api'

export default function Home() {
  const router = useRouter()
  
  const [companies, setCompanies] = useState([])
  const [selectedCompany, setSelectedCompany] = useState(null)
  const [allTables, setAllTables] = useState([])
  const [selectedTable, setSelectedTable] = useState(null)
  const [selectedTableMatrix, setSelectedTableMatrix] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [searchTerm, setSearchTerm] = useState('')
  const iframeRef = useRef(null);
  
  // For adding new companies
  const [addTicker, setAddTicker] = useState('')
  const [addName, setAddName] = useState('')

  // For extraction job
  const [jobId, setJobId] = useState(null);
  const [jobStatus, setJobStatus] = useState('');
  const [jobProgress, setJobProgress] = useState(0);
  const [jobMessage, setJobMessage] = useState('');
  const [jobSuccess, setJobSuccess] = useState(false);
  const today = new Date();
  const defaultEnd = today.toISOString().slice(0, 10);
  const defaultStart = '2024-08-01';
  const [startDate, setStartDate] = useState(defaultStart);
  const [endDate, setEndDate] = useState(defaultEnd);
  const [selectedFormTypes, setSelectedFormTypes] = useState(['8-K', '10-Q', '10-K']);

  // Load companies on mount
  useEffect(() => {
    fetch(`${API_BASE}/companies/`)
      .then(r => r.ok ? r.json() : [])
      .then(list => {
        setCompanies(list)
        // Auto-select UNH if available
        const unh = list.find(c => c.ticker === 'UNH')
        if (unh) {
          setSelectedCompany(unh)
        }
      })
      .catch(() => setCompanies([]))
  }, [])

  // Load all tables when a company is selected
  useEffect(() => {
    if (!selectedCompany) {
      setAllTables([])
      setSelectedTable(null)
      return
    }

    setLoading(true)
    setError('')
    
    fetch(`${API_BASE}/companies/${selectedCompany.ticker}/table-groups`)
      .then(r => {
        if (r.ok) return r.json()
        throw new Error(`HTTP ${r.status}`)
      })
      .then(groups => {
        setAllTables(groups)
        // Auto-select the first group
        if (groups.length > 0) {
          setSelectedTable(groups[0])
        }
      })
      .catch(err => {
        setError(`Failed to load table groups: ${err.message}`)
        setAllTables([])
      })
      .finally(() => setLoading(false))
  }, [selectedCompany])

  // Load the matrix for the selected table group
  useEffect(() => {
    if (!selectedTable) {
      setSelectedTableMatrix(null)
      return
    }

    setLoading(true)
    fetch(`${API_BASE}/table-groups/${selectedTable.id}/matrix`)
      .then(r => {
        if (r.ok) return r.json()
        throw new Error(`HTTP ${r.status}`)
      })
      .then(data => {
        setSelectedTableMatrix(data)
      })
      .catch(err => {
        setError(`Failed to load matrix for table group ${selectedTable.id}: ${err.message}`)
        setSelectedTableMatrix(null)
      })
      .finally(() => setLoading(false))
  }, [selectedTable])

  const startExtraction = async () => {
    if (!selectedCompany) return;
    if (selectedFormTypes.length === 0) {
      setError('Please select at least one form type');
      return;
    }
    setJobSuccess(false);
    setJobStatus('starting');
    setJobProgress(0);
    setError('');
    setLoading(true);
    
    try {
      const formTypesString = selectedFormTypes.join(',');
      const url = `${API_BASE}/extraction/start?company_ticker=${encodeURIComponent(selectedCompany.ticker)}&form_types=${encodeURIComponent(formTypesString)}&start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(endDate)}`;
      const res = await fetch(url, { method: 'POST' });
      if (!res.ok) throw new Error(await res.text());
      const { job_id } = await res.json();
      setJobId(job_id);

      setJobStatus('running');
      setJobMessage('Extraction started...');
      
      // Poll for job status
      const pollInterval = setInterval(async () => {
        try {
          const statusRes = await fetch(`${API_BASE}/jobs/${job_id}`);
          if (statusRes.status === 404) {
            // Job not found, might be a race condition or a deleted job
            // Stop polling to avoid spamming the console
            console.warn(`Job ${job_id} not found. Stopping polling.`);
            clearInterval(pollInterval);
            setError('Extraction job details could not be retrieved.');
            setLoading(false);
            return;
          }
          if (!statusRes.ok) return; // Silently fail and continue polling
          
          const job = await statusRes.json();
          setJobStatus(job.status);
          
          // Estimate progress
          const progress = job.job_metadata?.progress || 0;
          setJobProgress(progress);
          
          if (job.status === 'completed') {
            clearInterval(pollInterval);
            setJobProgress(100);
            setJobSuccess(true);
            setJobMessage('Extraction completed! Now generating highlighting data...');
            
            // This is the crucial missing step:
            await process_new_filings(job.id);

            // Reload all tables for the company
            const tablesResponse = await fetch(`${API_BASE}/companies/${selectedCompany.ticker}/table-groups`);
            if (tablesResponse.ok) {
              const tables = await tablesResponse.json();
              setAllTables(tables);
              if (tables.length > 0) {
                setSelectedTable(tables[0]);
              }
            }
            setLoading(false);
          } else if (job.status === 'failed') {
            clearInterval(pollInterval);
            setError(job.error_message || 'Extraction job failed');
            setLoading(false);
          }
        } catch (pollErr) {
          // Ignore polling errors
        }
      }, 3000); // Poll every 3 seconds

      // Store the interval ID to clear it if the component unmounts
      // setPollIntervalId(pollInterval); // This line was removed as per the edit hint

    } catch (e) {
      setError('Failed to start extraction');
      setJobStatus('failed');
      setLoading(false);
    }
  };

  const getPathTo = (element) => {
    if (element.id) return `//*[@id='${element.id}']`;
    if (element === document.body) return element.tagName;

    let ix = 0;
    const siblings = element.parentNode.childNodes;
    for (let i = 0; i < siblings.length; i++) {
      const sibling = siblings[i];
      if (sibling === element) return `${getPathTo(element.parentNode)}/${element.tagName}[${ix + 1}]`;
      if (sibling.nodeType === 1 && sibling.tagName === element.tagName) ix++;
    }
  };

  const generate_and_save_locations_for_filing = (filing, metrics) => {
    return new Promise((resolve, reject) => {
      const iframe = iframeRef.current;
      iframe.src = `http://127.0.0.1:8000/api/proxy?url=${encodeURIComponent(filing.file_url)}`;
      
      iframe.onload = () => {
        const iframeDoc = iframe.contentDocument;
        if (!iframeDoc) return reject(new Error("Could not access iframe content."));

        for (const metric of metrics) {
          const rowElement = Array.from(iframeDoc.getElementsByTagName('*')).find(el => el.textContent.includes(metric.original_label))?.closest('tr');
          if (rowElement) {
            const cellElement = Array.from(rowElement.getElementsByTagName('td')).find(cell => cell.textContent.includes(metric.value.toString()));
            if (cellElement) {
              const range = iframeDoc.createRange();
              range.selectNodeContents(cellElement);

              const locationData = {
                range_start_container_path: getPathTo(range.startContainer),
                range_start_offset: range.startOffset,
                range_end_container_path: getPathTo(range.endContainer),
                range_end_offset: range.endOffset,
                range_text: range.toString(),
                xpath: getPathTo(cellElement)
              };

              fetch(`${API_BASE}/metrics/${metric.id}/location`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(locationData)
              });
            }
          }
        }
        resolve();
      };
      iframe.onerror = () => reject(new Error("Failed to load document in iframe."));
    });
  };

  const process_new_filings = async (job_id) => {
    setJobMessage('Extraction complete. Now generating highlighting data...');
    const res = await fetch(`${API_BASE}/jobs/${job_id}/documents`);
    const new_filings = await res.json();
    
    for (const filing of new_filings) {
      setJobMessage(`Processing: ${filing.form_type} filed on ${filing.filing_date}`);
      const metrics_res = await fetch(`${API_BASE}/documents/${filing.id}/metrics`);
      const metrics = await metrics_res.json();
      
      await generate_and_save_locations_for_filing(filing, metrics);
    }
    
    setJobMessage('Highlighting data generated successfully!');
    setJobId(null); // Clear the job
  };

  const addCompany = async () => {
    if (!addTicker || !addName) return
    
    try {
      const res = await fetch(`${API_BASE}/companies/`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticker: addTicker.toUpperCase(), name: addName })
      })
      
      if (res.ok) {
        const created = await res.json()
        setCompanies(prev => [...prev, created])
        setSelectedCompany(created)
        setAddTicker('')
        setAddName('')
      }
    } catch (e) {
      setError('Failed to add company')
    }
  }

  const openDocumentViewer = async (cellData) => {
    if (!cellData || !cellData.id || !cellData.canonical_metric_name) {
      console.error("Cell data is missing ID or canonical metric name.", cellData);
      setError("Cannot open document: incomplete metric data.");
      return;
    }
    if (!selectedCompany) {
      console.error("No company selected.");
      setError("Please select a company first.");
      return;
    }

    try {
      const url = `${API_BASE}/metrics/${selectedCompany.ticker}/${cellData.canonical_metric_name}/with-tables`;
      const res = await fetch(url);

      if (!res.ok) {
        throw new Error(`Failed to fetch metric time series: ${res.status}`);
      }
      
      const timeSeries = await res.json();
      const metricDetails = timeSeries.find(m => m.id === cellData.id);

      if (!metricDetails) {
        console.error("Could not find the specific metric in the time series response.", { cellData, timeSeries });
        setError("Could not find details for the selected metric.");
        return;
      }
      
      if (!metricDetails.document_url) {
        console.error("Metric details are missing a document URL.", metricDetails);
        setError("Could not open document: URL not found in metric details.");
        return;
      }

      const row = cellData.cell_coordinates?.row ?? 0;
      const col = cellData.cell_coordinates?.col ?? 0;

      const params = new URLSearchParams({
        url: metricDetails.document_url,
        row: row.toString(),
        col: col.toString(),
        text: cellData.value ? cellData.value.toString() : ''
      });

      const viewerUrl = `/document-viewer?${params.toString()}`;
      window.open(viewerUrl, '_blank');

    } catch (err) {
      console.error("Error opening document viewer:", err);
      setError(err.message);
    }
  }

  const formatValue = (value, unit_multiplier, unit_text) => {
    if (value === null || value === undefined) return ''
    
    let displayValue = value
    
    // Apply unit multiplier for display
    if (unit_multiplier && unit_multiplier !== 1) {
      displayValue = value / unit_multiplier
    }
    
    // Format large numbers
    if (Math.abs(displayValue) >= 1000000000) {
      displayValue = (displayValue / 1000000000).toFixed(1) + 'B'
    } else if (Math.abs(displayValue) >= 1000000) {
      displayValue = (displayValue / 1000000).toFixed(1) + 'M'
    } else if (Math.abs(displayValue) >= 1000) {
      displayValue = (displayValue / 1000).toFixed(1) + 'K'
    } else {
      displayValue = displayValue.toFixed(2)
    }
    
    return displayValue
  }

  return (
    <div style={{ display: 'grid', gridTemplateRows: 'auto auto 1fr', gap: 16, padding: 24, minHeight: '100vh', background: '#f8fafc' }}>
      <iframe ref={iframeRef} style={{ display: 'none' }} />
      {/* Header */}
      <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
        <h1 style={{ margin: '0 0 8px 0', fontSize: '28px', fontWeight: 'bold', color: '#1f2937' }}>
          SEC Filing Data Explorer
        </h1>
        <p style={{ margin: 0, color: '#6b7280' }}>
          Explore financial data organized by semantic table groups with parameter matrices
        </p>
      </div>

      {/* Company Selection & Extraction Panel */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
        {/* Company Selection */}
        <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h2 style={{ margin: '0 0 16px 0', fontSize: '18px', fontWeight: '600', color: '#1f2937' }}>Companies</h2>
          
          {/* Company List */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 16 }}>
            {companies.map(c => (
              <button
                key={c.id}
                onClick={() => setSelectedCompany(c)}
                style={{ 
                  padding: '8px 12px', 
                  border: '1px solid #e5e7eb', 
                  borderRadius: '6px', 
                  background: selectedCompany?.id === c.id ? '#3b82f6' : '#fff',
                  color: selectedCompany?.id === c.id ? 'white' : '#374151',
                  cursor: 'pointer',
                  fontSize: '14px'
                }}
              >
                {c.ticker}
              </button>
            ))}
          </div>

          {/* Add Company */}
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <input
              placeholder="Ticker"
              value={addTicker}
              onChange={(e) => setAddTicker(e.target.value)}
              style={{ padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: '4px', fontSize: '14px', width: '80px' }}
            />
            <input
              placeholder="Company Name"
              value={addName}
              onChange={(e) => setAddName(e.target.value)}
              style={{ padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: '4px', fontSize: '14px', flex: 1 }}
            />
            <button
              onClick={addCompany}
              style={{ padding: '6px 12px', background: '#10b981', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer', fontSize: '14px' }}
            >
              Add
            </button>
          </div>
        </div>

        {/* Extraction Panel */}
        <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h2 style={{ margin: '0 0 16px 0', fontSize: '18px', fontWeight: '600', color: '#1f2937' }}>
            Data Extraction
          </h2>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
              <div>
                <label style={{ fontSize: '14px', color: '#374151', display: 'block', marginBottom: '4px' }}>Form Types</label>
                <div style={{ display: 'flex', gap: '12px' }}>
                  {['8-K', '10-K', '10-Q'].map(formType => (
                    <label key={formType} style={{ display: 'flex', alignItems: 'center', fontSize: '14px' }}>
                      <input
                        type="checkbox"
                        checked={selectedFormTypes.includes(formType)}
                        onChange={() => {
                          setSelectedFormTypes(prev => 
                            prev.includes(formType) 
                              ? prev.filter(ft => ft !== formType)
                              : [...prev, formType]
                          );
                        }}
                        style={{ marginRight: '4px' }}
                      />
                      {formType}
                    </label>
                  ))}
                </div>
              </div>
              <div>
                <label style={{ fontSize: '14px', color: '#374151', display: 'block', marginBottom: '4px' }}>Start Date</label>
                <input
                  type="date"
                  value={startDate}
                  onChange={e => setStartDate(e.target.value)}
                  style={{ padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: '4px', fontSize: '14px' }}
                />
              </div>
              <div>
                <label style={{ fontSize: '14px', color: '#374151', display: 'block', marginBottom: '4px' }}>End Date</label>
                <input
                  type="date"
                  value={endDate}
                  onChange={e => setEndDate(e.target.value)}
                  style={{ padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: '4px', fontSize: '14px' }}
                />
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'flex-end', gap: '16px' }}>
              <button
                onClick={startExtraction}
                disabled={!selectedCompany || loading}
                style={{ 
                  padding: '8px 16px', 
                  background: (!selectedCompany || loading) ? '#9ca3af' : '#2563eb', 
                  color: 'white', 
                  border: 'none', 
                  borderRadius: '4px', 
                  cursor: (!selectedCompany || loading) ? 'not-allowed' : 'pointer', 
                  fontSize: '14px' 
                }}
              >
                {loading ? 'Working...' : 'Extract Data'}
              </button>
            </div>
          </div>
          {jobId && (
            <div style={{ marginTop: '16px' }}>
              <div style={{ fontSize: '14px', color: '#374151' }}>
                Job Status: {jobStatus} {jobStatus === 'running' && `(${jobProgress}%)`}
                {jobSuccess && <span style={{ color: '#16a34a', marginLeft: '8px' }}>✓ Success</span>}
              </div>
              <div style={{ height: '8px', background: '#e5e7eb', borderRadius: '4px', overflow: 'hidden', marginTop: '8px' }}>
                <div style={{ height: '100%', width: `${jobProgress}%`, background: '#3b82f6', transition: 'width 0.3s ease' }} />
              </div>
              <div style={{ fontSize: '12px', color: '#6b7280', marginTop: '4px' }}>{jobMessage}</div>
            </div>
          )}
        </div>
      </div>

      {/* Table List and Viewer */}
      <div style={{ display: 'grid', gridTemplateColumns: '400px 1fr', gap: 16, overflow: 'hidden' }}>
        {/* Table List */}
        <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)', display: 'flex', flexDirection: 'column', maxHeight: '70vh' }}>
          <h2 style={{ margin: '0 0 16px 0', fontSize: '18px', fontWeight: '600', color: '#1f2937' }}>
            Table Groups ({allTables.length})
          </h2>
          <div style={{ flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 8 }}>
            {allTables.map(table => (
              <button
                key={table.id}
                onClick={() => setSelectedTable(table)}
                style={{
                  padding: '12px',
                  border: '1px solid #e5e7eb',
                  borderRadius: '6px',
                  background: selectedTable?.id === table.id ? '#eff6ff' : '#fff',
                  borderColor: selectedTable?.id === table.id ? '#3b82f6' : '#e5e7eb',
                  cursor: 'pointer',
                  textAlign: 'left'
                }}
              >
                <div style={{ fontWeight: '500', fontSize: '14px', color: '#1f2937', marginBottom: '4px' }}>
                  {table.group_name}
                </div>
                <div style={{ fontSize: '12px', color: '#6b7280' }}>
                  {`${table.metric_count} parameters × ${table.table_count} tables`}
                </div>
              </button>
            ))}
            {allTables.length === 0 && !loading && (
              <div style={{ padding: '20px', textAlign: 'center', color: '#6b7280', fontSize: '14px' }}>
                No table groups found.
              </div>
            )}
          </div>
        </div>

        {/* Selected Table Viewer */}
        <div style={{ background: 'white', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)', overflow: 'hidden' }}>
          {selectedTableMatrix ? (
            <div>
              {/* Matrix Header */}
              <div style={{ padding: '20px', borderBottom: '1px solid #e5e7eb' }}>
                <h2 style={{ margin: 0, fontSize: '18px', fontWeight: '600', color: '#1f2937' }}>
                  {selectedTableMatrix.group_name}
                </h2>
                <div style={{ fontSize: '14px', color: '#6b7280', marginTop: '4px' }}>
                  {selectedTableMatrix.parameters.length} parameters × {selectedTableMatrix.time_periods.length} periods
                </div>
              </div>
              {/* Table Matrix */}
              <div style={{ overflow: 'auto', maxHeight: '600px' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', tableLayout: 'fixed' }}>
                  <thead style={{ background: '#f9fafb', position: 'sticky', top: 0, zIndex: 2 }}>
                    <tr>
                      <th style={{ padding: '12px 16px', textAlign: 'left', fontSize: '12px', fontWeight: '600', color: '#374151', borderBottom: '2px solid #e5e7eb', width: '300px' }}>Parameter</th>
                      {selectedTableMatrix.time_periods.map((period, idx) => (
                        <th key={idx} style={{ padding: '12px 16px', textAlign: 'center', fontSize: '12px', fontWeight: '600', color: '#374151', borderBottom: '2px solid #e5e7eb', width: '140px' }}>
                          <div>{period.date}</div>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(
                      selectedTableMatrix.matrix.reduce((acc, row) => {
                        if (!row[0]) return acc; // Skip empty rows
                        const section = row[0].section_header || 'General';
                        if (!acc[section]) acc[section] = [];
                        acc[section].push(row);
                        return acc;
                      }, {})
                    ).map(([section, rows]) => (
                      <React.Fragment key={section}>
                        <tr>
                          <td colSpan={selectedTableMatrix.time_periods.length + 1} style={{ padding: '10px 16px', background: '#f3f4f6', fontWeight: 'bold', color: '#1f2937', borderBottom: '1px solid #e5e7eb', borderTop: '1px solid #e5e7eb' }}>
                            {section}
                          </td>
                        </tr>
                        {rows.map((row, paramIdx) => (
                          <tr key={row[0].original_label} style={{ background: paramIdx % 2 === 0 ? 'white' : '#f9fafb' }}>
                            <td style={{ padding: '16px', fontSize: '14px', fontWeight: '500', color: '#1f2937', borderBottom: '1px solid #e5e7eb' }}>
                              {row[0].original_label}
                            </td>
                            {row.map((cellData, periodIdx) => (
                              <td 
                                key={periodIdx} 
                                style={{ 
                                  padding: '16px', 
                                  fontSize: '14px', 
                                  textAlign: 'center', 
                                  borderBottom: '1px solid #e5e7eb',
                                  cursor: cellData ? 'pointer' : 'default'
                                }}
                                onClick={() => cellData && openDocumentViewer(cellData)}
                                onMouseEnter={(e) => { if (cellData) e.target.style.backgroundColor = '#eff6ff'; }}
                                onMouseLeave={(e) => { if (cellData) e.target.style.backgroundColor = 'inherit'; }}
                              >
                                {cellData ? (
                                  <div style={{ fontWeight: '600' }}>
                                    {formatValue(cellData.value, cellData.unit_multiplier, cellData.unit_text)}
                                  </div>
                                ) : (
                                  <div style={{ color: '#d1d5db' }}>—</div>
                                )}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </React.Fragment>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : (
            <div style={{ padding: '40px', textAlign: 'center', color: '#6b7280' }}>
              Select a table group to view its parameter matrix.
            </div>
          )}
        </div>
      </div>

      {/* Loading & Error States */}
      {loading && (
        <div style={{ 
          background: 'white', 
          padding: '40px', 
          borderRadius: '8px', 
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)', 
          textAlign: 'center' 
        }}>
          <div style={{ marginBottom: '12px' }}>Loading...</div>
          <div style={{ fontSize: '14px', color: '#6b7280' }}>Processing financial data</div>
        </div>
      )}

      {error && (
        <div style={{ 
          background: '#fef2f2', 
          border: '1px solid #fecaca', 
          borderRadius: '8px', 
          padding: '16px', 
          color: '#dc2626' 
        }}>
          <div style={{ fontWeight: '500', marginBottom: '4px' }}>Error</div>
          <div style={{ fontSize: '14px' }}>{error}</div>
        </div>
      )}

      {!selectedCompany && !loading && (
        <div style={{ 
          background: 'white', 
          padding: '40px', 
          borderRadius: '8px', 
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)', 
          textAlign: 'center' 
        }}>
          <h3 style={{ margin: '0 0 16px 0', fontSize: '18px', color: '#1f2937' }}>Welcome to SEC Data Explorer</h3>
          <p style={{ margin: '0 0 16px 0', color: '#6b7280' }}>
            Select a company above to explore financial data organized by semantic table groups
          </p>
          <div style={{ fontSize: '14px', color: '#6b7280' }}>
            • Table groups contain semantically similar tables across SEC filings<br/>
            • Parameter matrices show data evolution over time<br/>
            • Click any value to view the source document with highlighting
          </div>
        </div>
      )}
    </div>
  )
}