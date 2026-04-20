import { BrowserRouter, Routes, Route } from "react-router-dom";
import { EventProvider } from "./data/EventProvider";
import NavBar from "./components/NavBar";
import TaskDetail from "./pages/TaskDetail";
import JobDetail from "./pages/JobDetail";
import JobList from "./pages/JobList";
import ClusterDetail from "./pages/ClusterDetail";
import PerfOverview from "./pages/PerfOverview";
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
          <Route path="/jobs/:jobId/summary" element={<PerfOverview />} />
          <Route path="/clusters/:clusterId" element={<ClusterDetail />} />
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
