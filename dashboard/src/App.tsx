import { BrowserRouter, Routes, Route } from "react-router-dom";
import { EventProvider } from "./data/EventProvider";
import TaskDetail from "./pages/TaskDetail";
import JobDetail from "./pages/JobDetail";
import JobList from "./pages/JobList";
import ClusterDetail from "./pages/ClusterDetail";
import PerfOverview from "./pages/PerfOverview";
import NotImplemented from "./pages/NotImplemented";

export default function App() {
  return (
    <EventProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<JobList />} />
          <Route path="/cluster/:clusterId" element={<ClusterDetail />} />
          <Route path="/job/:jobId/task/:taskId" element={<TaskDetail />} />
          <Route path="/job/:jobId/perf-overview" element={<PerfOverview />} />
          <Route path="/job/:jobId" element={<JobDetail />} />
          <Route path="*" element={<NotImplemented />} />
        </Routes>
      </BrowserRouter>
    </EventProvider>
  );
}
