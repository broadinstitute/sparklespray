import { Link, useLocation } from "react-router-dom";

export interface TabItem {
  label: string;
  href: string;
  matchExact?: boolean;
}

export default function TabBar({ tabs }: { tabs: TabItem[] }) {
  const location = useLocation();

  return (
    <div
      style={{
        display: "flex",
        gap: 0,
        borderBottom: "2px solid #e0e0e0",
        marginBottom: "1.5rem",
        fontFamily: "'JetBrains Mono', monospace",
      }}
    >
      {tabs.map((tab) => {
        const isActive = tab.matchExact
          ? location.pathname === tab.href
          : location.pathname === tab.href ||
            location.pathname.startsWith(tab.href + "/");
        return (
          <Link
            key={tab.href}
            to={tab.href}
            style={{
              display: "block",
              textDecoration: "none",
              padding: "0.5rem 1.25rem",
              borderBottom: isActive
                ? "2px solid #1565c0"
                : "2px solid transparent",
              marginBottom: -2,
              fontSize: "0.85rem",
              fontWeight: isActive ? 700 : 400,
              color: isActive ? "#1565c0" : "#888",
              whiteSpace: "nowrap",
            }}
          >
            {tab.label}
          </Link>
        );
      })}
    </div>
  );
}
