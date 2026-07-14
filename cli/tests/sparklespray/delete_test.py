from unittest.mock import MagicMock

from sparklespray.commands.delete import delete


def test_delete_stops_cluster_by_default():
    cluster = MagicMock()
    jq = MagicMock()

    delete(cluster, jq, "job-1")

    cluster.stop_cluster.assert_called_once()
    cluster.delete_complete_requests.assert_called_once()
    jq.delete_job.assert_called_once_with("job-1")


def test_delete_can_keep_cluster_running():
    cluster = MagicMock()
    jq = MagicMock()

    delete(cluster, jq, "job-1", stop_cluster=False)

    # the lingering worker's cluster must NOT be torn down...
    cluster.stop_cluster.assert_not_called()
    # ...but the old bookkeeping is still cleared and terminal Batch jobs reaped
    cluster.delete_complete_requests.assert_called_once()
    jq.delete_job.assert_called_once_with("job-1")
