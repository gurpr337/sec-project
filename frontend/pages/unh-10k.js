import { useEffect, useState } from 'react'
import { useRouter } from 'next/router'

const API_BASE = 'http://localhost:8000/api'

export default function Unh10kPage() {
  const router = useRouter()
  const [documents, setDocuments] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [sortOrder, setSortOrder] = useState('desc')
  const [selectedDocument, setSelectedDocument] = useState(null)

  useEffect(() => {
    fetchDocuments()
  }, [sortOrder])

  const fetchDocuments = async () => {
    try {
      setLoading(true)
      const response = await fetch(`${API_BASE}/documents/unh/10k?sort_by_year=${sortOrder}`)
      if (!response.ok) {
        throw new Error('Failed to fetch documents')
      }
      const data = await response.json()
      setDocuments(data)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
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

  const handleDocumentClick = (document) => {
    setSelectedDocument(document);
  }

  const closeDocumentViewer = () => {
    setSelectedDocument(null);
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
              Loading UNH 10-K documents...
            </div>
          </div>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div style={{ display: 'grid', gridTemplateRows: 'auto auto auto 1fr', gap: '16px', padding: '24px', minHeight: '100vh', background: '#f8fafc' }}>
        <div style={{ background: 'white', padding: '24px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h1 style={{ margin: 0, fontSize: '28px', fontWeight: 700, color: '#1f2937' }}>SEC Filing Data Extraction System</h1>
          <p style={{ margin: '8px 0 0 0', color: '#6b7280' }}>Extract and analyze financial data from SEC filings with AI-powered table recognition</p>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '16px' }}>
          <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
            <div style={{ textAlign: 'center', color: '#dc2626', padding: '40px' }}>
              <div style={{ fontSize: '18px', fontWeight: 600, marginBottom: '8px' }}>Error</div>
              <p style={{ marginBottom: '16px' }}>{error}</p>
              <button
                onClick={fetchDocuments}
                style={{ padding: '10px 16px', background: '#3b82f6', color: 'white', border: 'none', borderRadius: '6px', fontSize: '14px', fontWeight: 500, cursor: 'pointer' }}
              >
                Retry
              </button>
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
        <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <h2 style={{ margin: '0 0 16px 0', fontSize: '18px', fontWeight: 600, color: '#1f2937' }}>UNH 10-K Documents ({documents.length})</h2>

          <div style={{ marginBottom: '16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div style={{ fontSize: '14px', color: '#6b7280' }}>
              Annual reports (10-K) for UnitedHealth Group spanning the past 20 years
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <label style={{ fontSize: '14px', color: '#6b7280' }}>Sort by year:</label>
              <select
                value={sortOrder}
                onChange={(e) => setSortOrder(e.target.value)}
                style={{ padding: '6px 12px', border: '1px solid #d1d5db', borderRadius: '6px', fontSize: '14px', background: 'white' }}
              >
                <option value="desc">Newest First</option>
                <option value="asc">Oldest First</option>
              </select>
            </div>
          </div>

          <div style={{ overflowX: 'auto', maxHeight: '70vh', overflowY: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead style={{ background: '#f9fafb', position: 'sticky', top: 0 }}>
                <tr>
                  <th style={{ padding: '12px', textAlign: 'left', fontSize: '12px', fontWeight: 500, color: '#6b7280', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: '1px solid #e5e7eb' }}>Filing Date</th>
                  <th style={{ padding: '12px', textAlign: 'left', fontSize: '12px', fontWeight: 500, color: '#6b7280', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: '1px solid #e5e7eb' }}>Form Type</th>
                  <th style={{ padding: '12px', textAlign: 'left', fontSize: '12px', fontWeight: 500, color: '#6b7280', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: '1px solid #e5e7eb' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {documents.map((doc) => (
                  <tr key={doc.id} style={{ borderBottom: '1px solid #e5e7eb', background: 'white' }}>
                    <td style={{ padding: '12px', fontSize: '14px', color: '#6b7280' }}>
                      {formatDate(doc.filing_date)}
                    </td>
                    <td style={{ padding: '12px', fontSize: '14px', color: '#6b7280' }}>
                      {doc.form_type}
                    </td>
                    <td style={{ padding: '12px' }}>
                      <button
                        onClick={() => handleDocumentClick(doc)}
                        style={{
                          color: '#2563eb',
                          textDecoration: 'none',
                          fontSize: '14px',
                          fontWeight: 500,
                          cursor: 'pointer',
                          background: 'none',
                          border: 'none',
                          padding: 0
                        }}
                        onMouseOver={(e) => e.target.style.color = '#1d4ed8'}
                        onMouseOut={(e) => e.target.style.color = '#2563eb'}
                      >
                        View Document
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {documents.length === 0 && (
            <div style={{ textAlign: 'center', padding: '40px', color: '#6b7280' }}>
              No documents found.
            </div>
          )}
        </div>

        <div style={{ background: 'white', padding: '20px', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)', display: 'flex', flexDirection: 'column', maxHeight: '70vh' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
            <h2 style={{ margin: 0, fontSize: '18px', fontWeight: 600, color: '#1f2937' }}>Document Viewer</h2>
            {selectedDocument && (
              <button
                onClick={closeDocumentViewer}
                style={{
                  background: 'none',
                  border: 'none',
                  color: '#6b7280',
                  fontSize: '20px',
                  cursor: 'pointer',
                  padding: '4px'
                }}
                onMouseOver={(e) => e.target.style.color = '#374151'}
                onMouseOut={(e) => e.target.style.color = '#6b7280'}
              >
                ×
              </button>
            )}
          </div>

          {selectedDocument ? (
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
              <div style={{ marginBottom: '12px', padding: '12px', background: '#f9fafb', borderRadius: '6px' }}>
                <div style={{ fontSize: '14px', fontWeight: 600, color: '#1f2937', marginBottom: '4px' }}>
                  {selectedDocument.year} - {selectedDocument.form_type}
                </div>
                <div style={{ fontSize: '12px', color: '#6b7280' }}>
                  Filed: {formatDate(selectedDocument.filing_date)} | {selectedDocument.accession_number}
                </div>
              </div>
              <div style={{ flex: 1, border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
                <iframe
                  src={`${API_BASE}/proxy?url=${encodeURIComponent(selectedDocument.file_url)}`}
                  style={{
                    width: '100%',
                    height: '100%',
                    border: 'none',
                    background: 'white'
                  }}
                  title={`SEC Document Viewer - ${selectedDocument.accession_number}`}
                />
              </div>
            </div>
          ) : (
            <div style={{ padding: '40px', textAlign: 'center', color: '#6b7280', background: '#f9fafb', borderRadius: '6px', flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <div>
                <div style={{ fontSize: '16px', marginBottom: '8px' }}>📄 Document Viewer</div>
                <div style={{ fontSize: '14px' }}>Click "View Document" on any filing to view it here</div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
