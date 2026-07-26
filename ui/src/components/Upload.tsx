import { useState } from "react";
import api from "../api";

interface Props {
  setJobId: (id: string) => void;
}

export default function Upload({ setJobId }: Props) {
  const [file, setFile] = useState<File | null>(null);

  const upload = async () => {
    if (!file) return;

    const formData = new FormData();
    formData.append("file", file);

    try {
      const { data } = await api.post("/upload", formData, {
        headers: {
          "Content-Type": "multipart/form-data",
        },
      });
      console.log(data);

      setJobId(data);
    } catch (err) {
      console.error(err);
      alert("Upload failed");
    }
  };

  return (
    <div>
      <input
        type="file"
        accept=".pdf,image/*"
        onChange={(e) => setFile(e.target.files?.[0] ?? null)}
      />

      <button onClick={upload} disabled={!file}>
        Upload
      </button>
    </div>
  );
}
