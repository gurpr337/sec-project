import { useRouter } from 'next/router';

export default function DocumentViewerPage() {
  const router = useRouter();
  const { url, row, col, text } = router.query;

  if (!url) {
    return <div>Loading...</div>;
  }
  
  const secDocumentUrl = `/sec-document.html?url=${encodeURIComponent(url)}&row=${row}&col=${col}&text=${encodeURIComponent(text || '')}`;

  return (
    <div style={{ height: '100vh', width: '100vw' }}>
      <iframe
        src={secDocumentUrl}
        style={{ width: '100%', height: '100%', border: 'none' }}
        title="SEC Document Viewer"
      />
    </div>
  );
}