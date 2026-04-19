import { useLocation } from "react-router-dom";

export default function NotImplemented() {
  const { pathname } = useLocation();
  const pageName =
    pathname === "/" ? "Home" : pathname.split("/").filter(Boolean).join(" / ");

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        height: "100vh",
        fontFamily: "monospace",
        fontSize: "1.2rem",
        color: "#555",
      }}
    >
      {pageName}: This page has not been implemented yet
    </div>
  );
}
