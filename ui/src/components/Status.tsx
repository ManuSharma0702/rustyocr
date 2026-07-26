import { useEffect, useState } from "react";
import api from "../api";

interface Props {
  jobId: string;
  setDownloadUrl: (url: string) => void;
}

export default function Status({
  jobId,
  setDownloadUrl,
}: Props) {
  const [status, setStatus] = useState("Waiting");

  useEffect(() => {
    if (!jobId) return;

    const interval = setInterval(async () => {
      const res = await api.get(`/jobs/${jobId}`);

      setStatus(res.data.status);

      if (res.data.status === "completed") {
        setDownloadUrl(res.data.download_url);
        clearInterval(interval);
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [jobId]);

  return (
    <div>
      <h3>Status</h3>
      <p>{status}</p>
    </div>
  );
}
