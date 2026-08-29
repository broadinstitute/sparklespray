import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { setApiKey } from "../api/client";

export default function InvalidApiKey() {
  const navigate = useNavigate();
  const [key, setKey] = useState("");

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = key.trim();
    if (!trimmed) return;
    setApiKey(trimmed);
    navigate("/");
  }

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        height: "100vh",
        fontFamily: "'IBM Plex Mono', monospace",
      }}
    >
      <div
        style={{
          width: 420,
          padding: 32,
          border: "1px solid #e0e0e0",
          borderRadius: 10,
          textAlign: "center",
        }}
      >
        <h2 style={{ margin: "0 0 8px", color: "#b71c1c" }}>
          Authorization failure
        </h2>
        <p style={{ margin: "0 0 20px", color: "#555", fontSize: "0.9rem" }}>
          Your API key is missing or invalid. Please enter a valid API key to
          continue.
        </p>
        <form
          onSubmit={handleSubmit}
          style={{ display: "flex", flexDirection: "column", gap: 12 }}
        >
          <input
            type="text"
            value={key}
            onChange={(e) => setKey(e.target.value)}
            placeholder="API key"
            autoFocus
            style={{
              padding: "8px 10px",
              fontFamily: "'IBM Plex Mono', monospace",
              fontSize: "0.9rem",
              border: "1px solid #ccc",
              borderRadius: 6,
            }}
          />
          <button
            type="submit"
            style={{
              padding: "8px 10px",
              fontFamily: "'IBM Plex Mono', monospace",
              fontSize: "0.9rem",
              color: "#fff",
              background: "#1565c0",
              border: "none",
              borderRadius: 6,
              cursor: "pointer",
            }}
          >
            Save and continue
          </button>
        </form>
      </div>
    </div>
  );
}
