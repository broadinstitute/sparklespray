import { BrowserRouter, Routes, Route } from "react-router-dom";
import { EventProvider } from "./data/EventProvider";
import NavBar from "./components/NavBar";
import TaskDetail from "./pages/TaskDetail";
import JobDetail from "./pages/JobDetail";
import JobList from "./pages/JobList";
import ClusterDetail from "./pages/ClusterDetail";
import ClusterLogs from "./pages/ClusterLogs";
import WorkPoolDetail from "./pages/WorkPoolDetail";
import WorkerDetail from "./pages/WorkerDetail";
import BatchDetail from "./pages/BatchDetail";
import NotImplemented from "./pages/NotImplemented";

function AppRoutes() {
  return (
    <>
      <NavBar />
      <div style={{ paddingTop: 40 }}>
        <Routes>
          <Route path="/" element={<JobList />} />
          <Route path="/jobs/:jobId" element={<JobDetail />} />
          <Route path="/jobs/:jobId/tasks" element={<JobDetail />} />
          <Route path="/jobs/:jobId/tasks/:taskId" element={<TaskDetail />} />
          <Route
            path="/jobs/:jobId/tasks/:taskId/metrics"
            element={<TaskDetail />}
          />
          <Route
            path="/jobs/:jobId/tasks/:taskId/log"
            element={<TaskDetail />}
          />
          <Route path="/jobs/:jobId/summary" element={<JobDetail />} />
          <Route path="/jobs/:jobId/events" element={<JobDetail />} />
          <Route path="/workpools/:workpoolId" element={<WorkPoolDetail />} />
          <Route
            path="/workpools/:workpoolId/workers"
            element={<WorkPoolDetail />}
          />
          <Route
            path="/workpools/:workpoolId/workers/:workerId"
            element={<WorkerDetail />}
          />
          <Route
            path="/workpools/:workpoolId/batches"
            element={<WorkPoolDetail />}
          />
          <Route
            path="/workpools/:workpoolId/batches/:batchId"
            element={<BatchDetail />}
          />
          <Route
            path="/workpools/:workpoolId/jobs"
            element={<WorkPoolDetail />}
          />
          <Route
            path="/workpools/:workpoolId/events"
            element={<WorkPoolDetail />}
          />
          <Route path="/clusters/:clusterId" element={<ClusterDetail />} />
          <Route path="/clusters/:clusterId/logs" element={<ClusterLogs />} />
          <Route path="*" element={<NotImplemented />} />
        </Routes>
      </div>
    </>
  );
}

export default function App() {
  return (
    <EventProvider>
      <BrowserRouter>
        <AppRoutes />
      </BrowserRouter>
    </EventProvider>
  );
}
