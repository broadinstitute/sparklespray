import { useParams, Navigate } from "react-router-dom";

export default function ClusterLogs() {
  const { clusterId } = useParams<{ clusterId: string }>();
  return <Navigate to={`/clusters/${clusterId}`} replace />;
}
