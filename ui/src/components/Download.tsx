interface Props {
  url: string;
}

export default function Download({ url }: Props) {
  if (!url) return null;

  return (
    <div>
      <a href={url}>
        <button>Download OCR File</button>
      </a>
    </div>
  );
}
