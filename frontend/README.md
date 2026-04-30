# SEC Filing Dashboard (Frontend)

This is the interactive frontend for the SEC Filing Data Extraction System. It provides a professional-grade interface for managing financial data extraction jobs, visualizing normalized metrics, and exploring semantic table groups.

## Key Features

- **Financial Time-Series Visualization**: Uses **Recharts** to render dynamic, interactive charts of canonical financial metrics (Revenue, Net Income, etc.) extracted from SEC filings.
- **Asynchronous Job Monitoring**: A robust polling system that provides real-time progress updates for backend extraction tasks.
- **Semantic Data Explorer**: Interfaces with the backend's vector search layers to display grouped tables and normalized financial data.
- **Company Management**: Clean interface for adding companies via ticker/CIK and managing their data lifecycle.
- **Responsive Design**: Built with **Tailwind CSS** for a premium, responsive experience across devices.

## Tech Stack

- **Framework**: [Next.js](https://nextjs.org) (App Router/Pages)
- **State Management**: React Hooks & Context
- **Visualization**: [Recharts](https://recharts.org)
- **Styling**: [Tailwind CSS](https://tailwindcss.com)
- **API Communication**: Fetch API with background polling logic

## Getting Started

### Prerequisites
- Node.js 16+
- Backend service running at `http://localhost:8000`

### Installation

1. Navigate to the frontend directory:
   ```bash
   cd frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the development server:
   ```bash
   npm run dev
   ```

The dashboard will be available at [http://localhost:3000](http://localhost:3000).

## Architecture Integration

The frontend is designed to be "status-aware." When an extraction job is triggered, the dashboard enters a polling state, communicating with the FastAPI backend to provide the user with granular feedback (e.g., "Fetching Filings," "Generating Embeddings," "Mapping Metrics") until the job reaches a terminal state.
