import { useEffect, useState } from 'react'
import { useRouter } from 'next/router'

const API_BASE = 'http://localhost:8000/api'

export default function UnhTableAnalysisPage() {
  const router = useRouter()
  const [documents, setDocuments] = useState([])
  const [selectedDocument, setSelectedDocument] = useState(null)
  const [loading, setLoading] = useState(true)
  const [analyzing, setAnalyzing] = useState(false)
  const [summary, setSummary] = useState(null)
  const [error, setError] = useState('')

  useEffect(() => {
    loadSummary()
  }, [])

  const loadSummary = async () => {
    try {
      const response = await fetch(`${API_BASE}/analysis/summary`)
      if (response.ok) {
        const data = await response.json()
        setSummary(data)
        loadDocuments()
      } else {
        // No analysis data yet
        loadDocuments()
      }
    } catch (err) {
      console.log('No existing analysis data')
      loadDocuments()
    }
  }

  const loadDocuments = async () => {
    try {
      const response = await fetch(`${API_BASE}/analysis/unh-documents`)
      if (!response.ok) throw new Error('Failed to load documents')
      const data = await response.json()
      setDocuments(data)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const startAnalysis = async () => {
    try {
      setAnalyzing(true)
      const response = await fetch(`${API_BASE}/analysis/analyze-unh`, {
        method: 'POST'
      })
      if (!response.ok) throw new Error('Failed to start analysis')
      const result = await response.json()
      alert('Analysis started! This will take several minutes. Refresh the page to check progress.')
    } catch (err) {
      alert(`Analysis failed: ${err.message}`)
    } finally {
      setAnalyzing(false)
    }
  }

  const formatDate = (dateString) => {
    if (!dateString) return 'N/A'
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric'
    })
  }

  const handleDocumentClick = async (document) => {
    setSelectedDocument(document)

    try {
      const response = await fetch(`${API_BASE}/analysis/documents/${document.id}/tables`)
      if (response.ok) {
        const tables = await response.json()

        // Load full data for all tables
        const tablesWithData = await Promise.all(
          tables.map(async (table) => {
            const fullData = await loadFullTableData(table)
            return fullData || table
          })
        )

        // Update the document with its tables (including full data)
        setSelectedDocument(prev => ({ ...prev, tables: tablesWithData }))
      }
    } catch (err) {
      console.error('Failed to load document tables:', err)
    }
  }

  const loadFullTableData = async (table) => {
    try {
      const response = await fetch(`${API_BASE}/analysis/tables/${table.id}`)
      if (response.ok) {
        const fullTable = await response.json()
        return fullTable
      }
    } catch (err) {
      console.error('Failed to load table details:', err)
    }
    return null
  }

  const renderParsedData = (parsedData, parsedHeaders) => {
    if (!parsedData || !Array.isArray(parsedData) || parsedData.length === 0) {
      return (
        <div style={{
          padding: '8px',
          textAlign: 'center',
          color: '#9ca3af',
          fontSize: '10px',
          background: '#f9fafb',
          border: '1px solid #e5e7eb',
          borderRadius: '4px'
        }}>
          No data
        </div>
      )
    }

    return (
      <div style={{ border: '1px solid #e5e7eb', borderRadius: '4px', overflow: 'hidden', fontSize: '10px' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          {parsedHeaders && Array.isArray(parsedHeaders) && parsedHeaders.length > 0 && (
            <thead style={{ background: '#f9fafb' }}>
              <tr>
                {parsedHeaders.map((header, idx) => (
                  <th key={idx} style={{
                    padding: '4px',
                    textAlign: 'left',
                    fontWeight: 600,
                    color: '#374151',
                    borderBottom: '1px solid #e5e7eb',
                    fontSize: '8px',
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    maxWidth: '120px'
                  }}>
                    {header && typeof header === 'string' && header.length > 18 ? header.substring(0, 18) + '...' : (header || '')}
                  </th>
                ))}
              </tr>
            </thead>
          )}
          <tbody>
            {parsedData.map((row, rowIdx) => {  // Show all rows
              if (!Array.isArray(row)) return null
              return (
                <tr key={rowIdx}>
                  {row.map((cell, cellIdx) => (  // Show all columns
                    <td key={cellIdx} style={{
                      padding: '4px',
                      color: '#374151',
                      borderRight: cellIdx < row.length - 1 ? '1px solid #f1f5f9' : 'none',
                      fontSize: '8px',
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      maxWidth: '120px'
                    }}>
                      {cell && typeof cell === 'object' && cell.hasOwnProperty('text')
                        ? (cell.text && typeof cell.text === 'string' && cell.text.length > 20 ? cell.text.substring(0, 20) + '...' : (cell.text || '-'))
                        : (typeof cell === 'string' && cell.length > 20 ? cell.substring(0, 20) + '...' : (cell || '-'))
                      }
                    </td>
                  ))}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    )
  }

  if (loading) {
    return (
      <div style={{ display: 'grid', gridTemplateRows: 'auto auto auto 1fr', gap: '16px', padding: '24px', minHeight: '100vh', background: '#f8fafc' }}>
        <div style={{ background: 'white', padding: '24px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h1 style={{ margin: 0, fontSize: '28px', fontWeight: 700, color: '#1f2937' }}>SEC Filing Data Extraction System</h1>
          <p style={{ margin: '8px 0 0 0', color: '#6b7280' }}>Extract and analyze financial data from SEC filings with AI-powered table recognition</p>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '16px' }}>
          <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
            <div style={{ textAlign: 'center', color: '#6b7280', padding: '40px' }}>
              Loading UNH table analysis...
            </div>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div style={{ display: 'grid', gridTemplateRows: 'auto auto auto 1fr', gap: '16px', padding: '24px', minHeight: '100vh', background: '#f8fafc' }}>
      <div style={{ background: 'white', padding: '24px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
        <h1 style={{ margin: 0, fontSize: '28px', fontWeight: 700, color: '#1f2937' }}>SEC Filing Data Extraction System</h1>
        <p style={{ margin: '8px 0 0 0', color: '#6b7280' }}>Extract and analyze financial data from SEC filings with AI-powered table recognition</p>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '16px' }}>
        {/* Left Panel - Documents */}
        <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
            <h2 style={{ margin: 0, fontSize: '18px', fontWeight: 600, color: '#1f2937' }}>
              UNH 10-K Documents {summary && `(${summary.total_documents})`}
            </h2>
            {(!summary || summary.total_tables === 0) && (
              <button
                onClick={startAnalysis}
                disabled={analyzing}
                style={{
                  padding: '8px 16px',
                  background: analyzing ? '#9ca3af' : '#3b82f6',
                  color: 'white',
                  border: 'none',
                  borderRadius: '6px',
                  fontSize: '14px',
                  fontWeight: 500,
                  cursor: analyzing ? 'not-allowed' : 'pointer'
                }}
              >
                {analyzing ? 'Analyzing...' : 'Start Analysis'}
              </button>
            )}
          </div>

          {summary && (
            <div style={{ marginBottom: '16px', padding: '12px', background: '#f0f9ff', borderRadius: '6px' }}>
              <div style={{ fontSize: '14px', fontWeight: 600, color: '#0369a1', marginBottom: '8px' }}>
                Analysis Complete: {summary.total_tables} tables across {summary.total_documents} documents
              </div>
              <div style={{ display: 'flex', gap: '12px', fontSize: '12px' }}>
                {Object.entries(summary.type_breakdown).map(([type, count]) => (
                  <span key={type} style={{ color: '#374151' }}>
                    {type}: {count}
                  </span>
                ))}
              </div>
            </div>
          )}

          <div style={{ maxHeight: '60vh', overflowY: 'auto' }}>
            {documents.map((doc) => (
              <div
                key={doc.id}
                onClick={() => handleDocumentClick(doc)}
                style={{
                  padding: '12px',
                  border: '1px solid #e5e7eb',
                  borderRadius: '6px',
                  marginBottom: '8px',
                  cursor: 'pointer',
                  background: selectedDocument?.id === doc.id ? '#eff6ff' : 'white'
                }}
              >
                <div style={{ fontSize: '14px', fontWeight: 600, color: '#1f2937', marginBottom: '4px' }}>
                  {doc.year} - 10-K Filing
                </div>
                <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>
                  Filed: {formatDate(doc.filing_date)}
                </div>
                {doc.table_count > 0 && (
                  <div style={{ fontSize: '12px', color: '#059669' }}>
                    {doc.table_count} tables analyzed
                    {doc.type_breakdown.type_1b > 0 && (
                      <span style={{ marginLeft: '8px', color: '#dc2626' }}>
                        {doc.type_breakdown.type_1b} Type 1B
                      </span>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Right Panel - Table Analysis */}
        <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)', display: 'flex', flexDirection: 'column', }}>
          <h2 style={{ margin: '0 0 16px 0', fontSize: '18px', fontWeight: 600, color: '#1f2937' }}>Table Analysis</h2>

          {selectedDocument && selectedDocument.tables ? (
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
              <div style={{ marginBottom: '12px', padding: '12px', background: '#f9fafb', borderRadius: '6px' }}>
                <div style={{ fontSize: '14px', fontWeight: 600, color: '#1f2937' }}>
                  {selectedDocument.year} - {selectedDocument.accession_number}
                </div>
                <div style={{ fontSize: '12px', color: '#6b7280', marginTop: '4px' }}>
                  {selectedDocument.tables.length} tables analyzed
                </div>
              </div>

              <div style={{
                flex: 1,
                overflow: 'hidden',
                display: 'flex',
                flexDirection: 'column',
                maxHeight: 'calc(70vh - 80px)' // Account for header space - make it bigger
              }}>
                <div style={{
                  flex: 1,
                  overflowY: 'auto',
                  overflowX: 'hidden',
                  paddingRight: '4px'
                }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                    {selectedDocument.tables.map((table) => (
                      <div key={table.id} style={{
                        border: '1px solid #e5e7eb',
                        borderRadius: '6px',
                        padding: '12px',
                        background: 'white'
                      }}>
                        <div style={{ marginBottom: '8px', fontSize: '12px', fontWeight: 600, color: '#374151' }}>
                          Table {table.table_index}: {table.title || 'Untitled'}
                          <span style={{
                            marginLeft: '8px',
                            padding: '2px 6px',
                            fontSize: '10px',
                            fontWeight: 500,
                            borderRadius: '9999px',
                            background: table.table_type === 'type_1b' ? '#dcfce7' : table.table_type === 'type_1a' ? '#dbeafe' : '#f3f4f6',
                            color: table.table_type === 'type_1b' ? '#166534' : table.table_type === 'type_1a' ? '#1e40af' : '#374151'
                          }}>
                            {table.table_type}
                          </span>
                        </div>

                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                          {/* Original HTML */}
                          <div>
                            <div style={{ fontSize: '10px', fontWeight: 600, color: '#6b7280', marginBottom: '4px' }}>
                              Original HTML
                            </div>
                            <div
                              style={{
                                padding: '6px',
                                background: '#f9fafb',
                                border: '1px solid #e5e7eb',
                                borderRadius: '4px',
                                fontSize: '9px',
                                fontFamily: 'monospace',
                                whiteSpace: 'pre-wrap',
                                maxHeight: '400px',
                                overflowY: 'auto',
                                color: '#374151'
                              }}
                              dangerouslySetInnerHTML={{ __html: table.original_html }}
                            />
                          </div>

                          {/* Parsed Data */}
                          <div>
                            <div style={{ fontSize: '10px', fontWeight: 600, color: '#6b7280', marginBottom: '4px' }}>
                              Parsed Data ({table.parsed_data?.length || 0} rows)
                            </div>
                            <div style={{ maxHeight: '400px', overflowY: 'auto' }}>
                              {renderParsedData(table.parsed_data, table.parsed_headers)}
                            </div>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#6b7280', textAlign: 'center' }}>
              <div>
                <div style={{ fontSize: '16px', marginBottom: '8px' }}>📊 Table Analysis</div>
                <div style={{ fontSize: '14px' }}>
                  {(!summary || summary.total_tables === 0)
                    ? "Click 'Start Analysis' to analyze all tables in UNH 10-K documents"
                    : "Select a document and then a table to view the original HTML vs parsed data"
                  }
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
