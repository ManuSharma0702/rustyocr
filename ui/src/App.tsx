import { useState } from "react";
import Upload from "./components/Upload";
import Status from "./components/Status";
import Download from "./components/Download";

function App() {
  const [jobId, setJobId] = useState("");
  const [downloadUrl, setDownloadUrl] = useState("");

  return (
    <div
      style={{
        maxWidth: "600px",
        margin: "50px auto",
        padding: "20px",
        textAlign: "center",
        border: "1px solid #ddd",
        borderRadius: "8px",
      }}
    >
      <h1>RustyOCR</h1>

      <Upload setJobId={setJobId} />

      {jobId && (
        <>
          <p>
            <strong>Job ID:</strong> {jobId}
          </p>

          <Status
            jobId={jobId}
            setDownloadUrl={setDownloadUrl}
          />
        </>
      )}

      <Download url={downloadUrl} />
    </div>
  );
}

export default App;
