import { useEffect, useState, useRef } from 'react'
import { useRouter } from 'next/router'
import React from 'react';

const API_BASE = 'http://127.0.0.1:8000/api'

const MetricRow = ({ metric, headers }) => {
    return (
        <>
            <tr style={{ background: metric.is_section_header ? '#f3f4f6' : 'white' }}>
                <td style={{ paddingLeft: `${metric.level * 20}px`, fontWeight: metric.is_section_header ? 'bold' : 'normal' }}>
                    {metric.raw_name}
                </td>
                {headers.map(header => (
                    <td key={header.id} style={{ textAlign: 'center' }}>
                        {metric.data_points.find(dp => dp.header_id === header.id)?.value || ''}
                    </td>
                ))}
            </tr>
            {metric.children && metric.children.map(child => (
                <MetricRow key={child.id} metric={child} headers={headers} />
            ))}
        </>
    );
};

export default function Home() {
  const router = useRouter()
  
  const [companies, setCompanies] = useState([])
  const [selectedCompany, setSelectedCompany] = useState(null)
  const [tableGroups, setTableGroups] = useState([])
  const [selectedTableGroup, setSelectedTableGroup] = useState(null)
  const [tableEvolution, setTableEvolution] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const iframeRef = useRef(null);
  
  const [addTicker, setAddTicker] = useState('')
  const [addName, setAddName] = useState('')

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

  useEffect(() => {
    fetch(`${API_BASE}/companies/`)
      .then(r => r.ok ? r.json() : [])
      .then(list => {
        setCompanies(list)
        const unh = list.find(c => c.ticker === 'UNH')
        if (unh) {
          setSelectedCompany(unh)
        }
      })
      .catch(() => setCompanies([]))
  }, [])

  useEffect(() => {
    if (!selectedCompany) {
      setTableGroups([])
      setSelectedTableGroup(null)
      return
    }

    setLoading(true)
    setError('')
    
    // This endpoint needs to be created
    fetch(`${API_BASE}/companies/${selectedCompany.ticker}/financial-table-groups`)
      .then(r => r.ok ? r.json() : [])
      .then(groups => {
        setTableGroups(groups)
        if (groups.length > 0) {
          // Default to table group 8 (balance sheet) if it exists, otherwise first group
          const balanceSheetGroup = groups.find(g => g.id === 8)
          setSelectedTableGroup(balanceSheetGroup || groups[0])
        }
      })
      .catch(err => {
        setError(`Failed to load table groups: ${err.message}`)
        setTableGroups([])
      })
      .finally(() => setLoading(false))
  }, [selectedCompany])

  useEffect(() => {
    if (!selectedTableGroup) {
      setTableEvolution(null)
      return
    }

    setLoading(true)
    // Load table evolution data for the entire group
    fetch(`${API_BASE}/financial-tables/groups/${selectedTableGroup.id}/evolution`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then(data => {
        setTableEvolution(data.evolution)
      })
      .catch(err => {
        setError(`Failed to load table evolution: ${err.message}`)
        setTableEvolution(null)
      })
      .finally(() => setLoading(false))
  }, [selectedTableGroup])

  const startExtraction = async () => {
    if (!selectedCompany || selectedFormTypes.length === 0) {
      setError('Please select a company and at least one form type')
      return
    }

    try {
      setJobStatus('running')
      setJobProgress(0)
      setJobMessage('Starting extraction...')
      setError('')

      const url = `${API_BASE}/extraction/start?company_ticker=${encodeURIComponent(selectedCompany.ticker)}&form_types=${encodeURIComponent(selectedFormTypes.join(','))}&start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(endDate)}`

      const res = await fetch(url, { method: 'POST' })
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`)
      }

      const job = await res.json()
      setJobId(job.id)
      setJobMessage('Extraction job started...')

      // Poll for job status
      const pollJob = async () => {
        try {
          const statusRes = await fetch(`${API_BASE}/extraction/jobs/${job.id}`)
          if (!statusRes.ok) {
            throw new Error(`HTTP ${statusRes.status}`)
          }

          const jobData = await statusRes.json()

          setJobStatus(jobData.status)
          setJobProgress(jobData.progress || 0)
          setJobMessage(jobData.message || '')

          if (jobData.status === 'completed') {
            setJobSuccess(true)
            // Refresh table groups after completion
            if (selectedCompany) {
              fetch(`${API_BASE}/companies/${selectedCompany.ticker}/financial-table-groups`)
                .then(r => r.ok ? r.json() : [])
                .then(groups => setTableGroups(groups))
                .catch(err => console.error('Failed to refresh table groups:', err))
            }
          } else if (jobData.status === 'failed') {
            setError(jobData.error_message || 'Extraction failed')
          } else if (jobData.status === 'running') {
            // Continue polling
            setTimeout(pollJob, 3000)
          }
        } catch (err) {
          setError(`Failed to check job status: ${err.message}`)
        }
      }

      // Start polling
      setTimeout(pollJob, 2000)

    } catch (err) {
      setError(`Failed to start extraction: ${err.message}`)
      setJobStatus('')
    }
  }

  const addCompany = async () => {
    if (!addTicker || !addName) {
      setError('Please provide both ticker and company name')
      return
    }

    try {
      setError('')

      const res = await fetch(`${API_BASE}/companies/`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticker: addTicker, name: addName })
      })

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`)
      }

      const created = await res.json()
      setCompanies(prev => [...prev, created])
      setSelectedCompany(created)
      setAddTicker('')
      setAddName('')

    } catch (err) {
      setError(`Failed to add company: ${err.message}`)
    }
  }

  return (
    <div style={{ display: 'grid', gridTemplateRows: 'auto auto auto 1fr', gap: 16, padding: 24, minHeight: '100vh', background: '#f8fafc' }}>
      {/* Header */}
      <div style={{ background: 'white', padding: '24px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
        <h1 style={{ margin: 0, fontSize: '28px', fontWeight: '700', color: '#1f2937' }}>
          SEC Filing Data Extraction System
        </h1>
        <p style={{ margin: '8px 0 0 0', color: '#6b7280' }}>
          Extract and analyze financial data from SEC filings with AI-powered table recognition
        </p>
      </div>

      {/* Company Selection and Management */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 400px', gap: 16 }}>
        {/* Company Selection */}
        <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h2 style={{ margin: '0 0 16px 0', fontSize: '18px', fontWeight: '600', color: '#1f2937' }}>
            Company Selection
          </h2>

          <div style={{ marginBottom: '16px' }}>
            <label style={{ display: 'block', marginBottom: '8px', fontSize: '14px', fontWeight: '500', color: '#374151' }}>
              Select Company:
            </label>
            <select
              value={selectedCompany?.ticker || ''}
              onChange={(e) => {
                const company = companies.find(c => c.ticker === e.target.value)
                setSelectedCompany(company || null)
              }}
              style={{
                width: '100%',
                padding: '8px 12px',
                border: '1px solid #d1d5db',
                borderRadius: '6px',
                fontSize: '14px',
                background: 'white'
              }}
            >
              <option value="">Select a company...</option>
              {companies.map(company => (
                <option key={company.ticker} value={company.ticker}>
                  {company.ticker} - {company.name}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Add Company */}
        <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h2 style={{ margin: '0 0 16px 0', fontSize: '18px', fontWeight: '600', color: '#1f2937' }}>
            Add Company
          </h2>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div>
              <label style={{ display: 'block', marginBottom: '4px', fontSize: '14px', fontWeight: '500', color: '#374151' }}>
                Ticker Symbol:
              </label>
              <input
                type="text"
                value={addTicker}
                onChange={(e) => setAddTicker(e.target.value.toUpperCase())}
                placeholder="e.g., AAPL"
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  border: '1px solid #d1d5db',
                  borderRadius: '6px',
                  fontSize: '14px'
                }}
              />
            </div>

            <div>
              <label style={{ display: 'block', marginBottom: '4px', fontSize: '14px', fontWeight: '500', color: '#374151' }}>
                Company Name:
              </label>
              <input
                type="text"
                value={addName}
                onChange={(e) => setAddName(e.target.value)}
                placeholder="e.g., Apple Inc."
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  border: '1px solid #d1d5db',
                  borderRadius: '6px',
                  fontSize: '14px'
                }}
              />
            </div>

            <button
              onClick={addCompany}
              disabled={!addTicker || !addName}
              style={{
                padding: '10px 16px',
                background: (!addTicker || !addName) ? '#9ca3af' : '#3b82f6',
                color: 'white',
                border: 'none',
                borderRadius: '6px',
                fontSize: '14px',
                fontWeight: '500',
                cursor: (!addTicker || !addName) ? 'not-allowed' : 'pointer'
              }}
            >
              Add Company
            </button>
          </div>
        </div>
      </div>

      {/* Extraction Controls */}
      {selectedCompany && (
        <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h2 style={{ margin: '0 0 16px 0', fontSize: '18px', fontWeight: '600', color: '#1f2937' }}>
            Data Extraction: {selectedCompany.name} ({selectedCompany.ticker})
          </h2>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 16, marginBottom: '16px' }}>
            <div>
              <label style={{ display: 'block', marginBottom: '4px', fontSize: '14px', fontWeight: '500', color: '#374151' }}>
                Start Date:
              </label>
              <input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  border: '1px solid #d1d5db',
                  borderRadius: '6px',
                  fontSize: '14px'
                }}
              />
            </div>

            <div>
              <label style={{ display: 'block', marginBottom: '4px', fontSize: '14px', fontWeight: '500', color: '#374151' }}>
                End Date:
              </label>
              <input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  border: '1px solid #d1d5db',
                  borderRadius: '6px',
                  fontSize: '14px'
                }}
              />
            </div>

            <div>
              <label style={{ display: 'block', marginBottom: '4px', fontSize: '14px', fontWeight: '500', color: '#374151' }}>
                Form Types:
              </label>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                {['8-K', '10-Q', '10-K'].map(formType => (
                  <label key={formType} style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: '14px' }}>
                    <input
                      type="checkbox"
                      checked={selectedFormTypes.includes(formType)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedFormTypes([...selectedFormTypes, formType])
                        } else {
                          setSelectedFormTypes(selectedFormTypes.filter(t => t !== formType))
                        }
                      }}
                    />
                    {formType}
                  </label>
                ))}
              </div>
            </div>

            <div style={{ display: 'flex', alignItems: 'end' }}>
              <button
                onClick={startExtraction}
                disabled={jobStatus === 'running'}
                style={{
                  padding: '10px 20px',
                  background: jobStatus === 'running' ? '#9ca3af' : '#10b981',
                  color: 'white',
                  border: 'none',
                  borderRadius: '6px',
                  fontSize: '14px',
                  fontWeight: '500',
                  cursor: jobStatus === 'running' ? 'not-allowed' : 'pointer',
                  width: '100%'
                }}
              >
                {jobStatus === 'running' ? 'Extracting...' : 'Start Extraction'}
              </button>
            </div>
          </div>

          {/* Job Status */}
          {jobId && (
            <div style={{ padding: '16px', background: '#f9fafb', borderRadius: '6px', border: '1px solid #e5e7eb' }}>
              <h3 style={{ margin: '0 0 8px 0', fontSize: '16px', fontWeight: '600', color: '#1f2937' }}>
                Extraction Status
              </h3>

              <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: '8px' }}>
                <div style={{ flex: 1 }}>
                  <div style={{
                    width: '100%',
                    height: '8px',
                    background: '#e5e7eb',
                    borderRadius: '4px',
                    overflow: 'hidden'
                  }}>
                    <div style={{
                      width: `${jobProgress}%`,
                      height: '100%',
                      background: jobSuccess ? '#10b981' : '#3b82f6',
                      transition: 'width 0.3s ease'
                    }} />
                  </div>
                </div>
                <span style={{ fontSize: '14px', fontWeight: '500', color: '#374151' }}>
                  {jobProgress}%
                </span>
              </div>

              <p style={{ margin: 0, fontSize: '14px', color: '#6b7280' }}>
                {jobMessage || `Job ${jobId}: ${jobStatus}`}
              </p>
            </div>
          )}
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '400px 1fr', gap: 16, overflow: 'hidden' }}>
        <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)', display: 'flex', flexDirection: 'column', maxHeight: '70vh' }}>
          <h2 style={{ margin: '0 0 16px 0', fontSize: '18px', fontWeight: '600', color: '#1f2937' }}>
            Table Groups ({tableGroups.length})
          </h2>
          <div style={{ flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 8 }}>
            {tableGroups.map(group => (
              <button
                key={group.id}
                onClick={() => setSelectedTableGroup(group)}
                style={{
                  padding: '12px',
                  border: '1px solid #e5e7eb',
                  borderRadius: '6px',
                  background: selectedTableGroup?.id === group.id ? '#eff6ff' : '#fff',
                  borderColor: selectedTableGroup?.id === group.id ? '#3b82f6' : '#e5e7eb',
                  cursor: 'pointer',
                  textAlign: 'left'
                }}
              >
                <div style={{ fontWeight: '500', fontSize: '14px', color: '#1f2937', marginBottom: '4px' }}>
                  {group.name}
                </div>
              </button>
            ))}
          </div>
        </div>

        <div style={{ background: 'white', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)', overflow: 'hidden' }}>
          {tableEvolution && tableEvolution.length > 0 ? (
            <div>
              <div style={{ padding: '20px', borderBottom: '1px solid #e5e7eb' }}>
                <h2 style={{ margin: 0, fontSize: '18px', fontWeight: '600', color: '#1f2937' }}>
                  Table Evolution: {selectedTableGroup?.name}
                </h2>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px', color: '#6b7280' }}>
                  {tableEvolution.length} documents • Chronological view
                </p>
              </div>
              <div style={{ overflow: 'auto', maxHeight: '600px' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                  <thead>
                    <tr style={{ background: '#f9fafb' }}>
                      <th style={{ padding: '8px', border: '1px solid #e5e7eb', fontWeight: '600', textAlign: 'left', position: 'sticky', left: 0, background: '#f9fafb', zIndex: 10 }}>
                        Metric
                      </th>
                      {/* Group columns by document */}
                      {tableEvolution.map((doc, docIndex) => (
                        <th
                          key={`doc-header-${doc.document_id}-${docIndex}`}
                          colSpan={doc.column_headers.length}
                          style={{
                            padding: '8px',
                            border: '1px solid #e5e7eb',
                            fontWeight: '600',
                            textAlign: 'center',
                            background: '#eff6ff',
                            color: '#1e40af'
                          }}
                        >
                          <div>{doc.document_type}</div>
                          <div style={{ fontSize: '10px', fontWeight: '400', marginTop: '2px' }}>
                            {new Date(doc.document_date).toLocaleDateString()}
                          </div>
                        </th>
                      ))}
                    </tr>
                    <tr style={{ background: '#f9fafb' }}>
                      <th style={{ padding: '8px', border: '1px solid #e5e7eb', position: 'sticky', left: 0, background: '#f9fafb', zIndex: 10 }}></th>
                      {/* Individual column headers */}
                      {tableEvolution.map((doc, docIndex) => (
                        doc.column_headers.map((header, headerIndex) => (
                          <th
                            key={`col-header-${doc.document_id}-${docIndex}-${headerIndex}-${header}`}
                            style={{
                              padding: '6px',
                              border: '1px solid #e5e7eb',
                              fontWeight: '500',
                              fontSize: '10px',
                              textAlign: 'center',
                              background: '#f8fafc',
                              writingMode: 'horizontal-tb',
                              whiteSpace: 'nowrap',
                              overflow: 'hidden',
                              textOverflow: 'ellipsis',
                              maxWidth: '120px'
                            }}
                            title={header}
                          >
                            {header}
                          </th>
                        ))
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {/* Group metrics by section and preserve original ordering */}
                    {(() => {
                      // Collect all metrics with their original ordering information
                      const allMetrics = [];
                      const sectionOrder = new Map(); // Track order of first appearance of each section

                      const metricMap = new Map(); // Track unique metrics by flattened_name

                      tableEvolution.forEach(doc => {
                        doc.metrics.forEach((metric, metricIndex) => {
                          // Parse section from flattened name (e.g., "Revenues - unaffiliated customers: :: Premiums :: UnitedHealthcare")
                          const parts = metric.flattened_name.split(' :: ');
                          let section = 'General';
                          let metricName = metric.flattened_name;

                          if (parts.length >= 3) {
                            // Type 2 table: "Section :: Metric :: Segment"
                            section = parts[0];
                            metricName = parts[1] + ' :: ' + parts[2]; // "Premiums :: UnitedHealthcare"
                          } else if (parts.length >= 2) {
                            // Type 1 table: "Section :: Metric"
                            section = parts[0];
                            metricName = parts[1];
                          } else {
                            // No section delimiter found
                            metricName = metric.flattened_name;
                          }

                          // Track section order (first appearance)
                          if (!sectionOrder.has(section)) {
                            sectionOrder.set(section, sectionOrder.size);
                          }

                          // Only add if we haven't seen this metric before
                          if (!metricMap.has(metric.flattened_name)) {
                            metricMap.set(metric.flattened_name, {
                              fullName: metric.flattened_name,
                              section: section,
                              metricName: metricName,
                              metric: metric,
                              doc: doc,
                              docIndex: tableEvolution.indexOf(doc),
                              metricIndex: metricIndex // Preserve original order within document
                            });
                          }
                        });
                      });

                      // Convert map to array
                      allMetrics.push(...metricMap.values());

                      // Group by section and sort sections by their first appearance order
                      const sectionGroups = {};
                      allMetrics.forEach(item => {
                        if (!sectionGroups[item.section]) {
                          sectionGroups[item.section] = [];
                        }
                        sectionGroups[item.section].push(item);
                      });

                      // Sort sections by their first appearance order, then metrics by their original order
                      const sortedSections = Object.keys(sectionGroups).sort((a, b) => {
                        return sectionOrder.get(a) - sectionOrder.get(b);
                      });

                      const rows = [];
                      sortedSections.forEach(sectionName => {
                        const sectionMetrics = sectionGroups[sectionName];

                        // Sort metrics within section by their original document order and metric index
                        sectionMetrics.sort((a, b) => {
                          if (a.docIndex !== b.docIndex) {
                            return a.docIndex - b.docIndex;
                          }
                          return a.metricIndex - b.metricIndex;
                        });

                        // Add section header row
                        rows.push(
                          <tr key={`section-header-${sectionName}-${sectionOrder.get(sectionName)}`} style={{ background: '#f8fafc', borderTop: '2px solid #e5e7eb' }}>
                            <td style={{
                              padding: '8px 8px',
                              border: '1px solid #e5e7eb',
                              fontWeight: 'bold',
                              fontSize: '13px',
                              position: 'sticky',
                              left: 0,
                              background: '#f8fafc',
                              zIndex: 5,
                              maxWidth: '200px',
                              wordWrap: 'break-word',
                              color: '#1f2937',
                              borderBottom: '1px solid #d1d5db'
                            }}>
                              {sectionName}
                            </td>
                            {/* Empty cells for section header spanning all data columns */}
                            {tableEvolution.map((doc, docIndex) => (
                              doc.column_headers.map((headerName, headerIndex) => (
                                <td
                                  key={`section-spacer-${sectionName}-${doc.document_id}-${headerIndex}`}
                                  style={{
                                    padding: '8px',
                                    border: '1px solid #e5e7eb',
                                    background: '#f8fafc',
                                    borderBottom: '1px solid #d1d5db'
                                  }}
                                />
                              ))
                            ))}
                          </tr>
                        );

                        // Add metric rows for this section
                        rows.push(...sectionMetrics.map((item, localMetricIndex) => (
                          <tr key={`metric-${item.docIndex}-${item.metricIndex}-${sectionName}-${localMetricIndex}-${item.fullName}`} style={{ borderBottom: '1px solid #f3f4f6' }}>
                            <td style={{
                              padding: '6px 8px 6px 24px', // Extra left padding for subsection metrics
                              border: '1px solid #e5e7eb',
                              fontWeight: '500',
                              position: 'sticky',
                              left: 0,
                              background: 'white',
                              zIndex: 5,
                              maxWidth: '200px',
                              wordWrap: 'break-word'
                            }}>
                              {item.metricName && item.metricName.trim() && item.metricName !== '*' && item.metricName !== '**'
                                ? item.metricName
                                : item.fullName || 'Unnamed Metric'}
                            </td>

                            {/* Render data points for each document */}
                            {tableEvolution.map((doc, docIndex) => {
                              const metric = doc.metrics.find(m => m.flattened_name === item.fullName);
                              return doc.column_headers.map((headerName, headerIndex) => {
                                const dataPoint = metric?.data_points[headerName];

                                return (
                                  <td
                                    key={`data-cell-${doc.document_id}-${docIndex}-${headerIndex}-${headerName}`}
                                    style={{
                                      padding: '6px',
                                      border: '1px solid #e5e7eb',
                                      textAlign: 'right',
                                      fontFamily: 'monospace',
                                      fontSize: '11px',
                                      background: dataPoint ? 'white' : '#fafafa',
                                      cursor: dataPoint?.cell_coordinates ? 'pointer' : 'default'
                                    }}
                                    onClick={() => {
                                      if (dataPoint?.cell_coordinates) {
                                        // Open document viewer in new tab
                                        const viewerUrl = `/document-viewer?url=${encodeURIComponent(doc.document_url)}&row=${dataPoint.cell_coordinates.row}&col=${dataPoint.cell_coordinates.col}&text=${encodeURIComponent(String(dataPoint.value || ''))}`;
                                        window.open(viewerUrl, '_blank');
                                      }
                                    }}
                                    title={dataPoint ? `Click to view source` : 'No data'}
                                  >
                                    {dataPoint ? (
                                      <span style={{ color: typeof dataPoint.value === 'number' ? '#059669' : '#6b7280' }}>
                                        {typeof dataPoint.value === 'number'
                                          ? dataPoint.value.toLocaleString()
                                          : String(dataPoint.value || '-')
                                        }
                                      </span>
                                    ) : (
                                      <span style={{ color: '#d1d5db' }}>—</span>
                                    )}
                                  </td>
                                );
                              });
                            })}
                          </tr>
                        )))
                      });

                      return rows;
                    })()}
                  </tbody>
                </table>
              </div>
            </div>
          ) : (
            <div style={{ padding: '40px', textAlign: 'center', color: '#6b7280' }}>
              {selectedTableGroup
                ? "Loading table evolution data..."
                : "Select a table group to view its evolution over time."
              }
            </div>
          )}
        </div>
      </div>

      {/* Error Display */}
      {error && (
        <div style={{
          background: '#fef2f2',
          border: '1px solid #fecaca',
          borderRadius: '6px',
          padding: '16px',
          marginTop: '16px'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ color: '#dc2626', fontSize: '16px' }}>⚠️</span>
            <span style={{ color: '#dc2626', fontWeight: '500' }}>Error</span>
          </div>
          <p style={{ margin: '8px 0 0 0', color: '#991b1b' }}>{error}</p>
        </div>
      )}

      {/* Loading Overlay */}
      {loading && (
        <div style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background: 'rgba(0, 0, 0, 0.5)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 1000
        }}>
          <div style={{
            background: 'white',
            padding: '24px',
            borderRadius: '8px',
            boxShadow: '0 10px 25px rgba(0, 0, 0, 0.2)',
            textAlign: 'center'
          }}>
            <div style={{
              width: '32px',
              height: '32px',
              border: '3px solid #e5e7eb',
              borderTop: '3px solid #3b82f6',
              borderRadius: '50%',
              animation: 'spin 1s linear infinite',
              margin: '0 auto 16px'
            }} />
            <p style={{ margin: 0, color: '#374151', fontWeight: '500' }}>Loading...</p>
          </div>
        </div>
      )}

      <style jsx>{`
        @keyframes spin {
          0% { transform: rotate(0deg); }
          100% { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  )
}
