from unittest import TestCase


from servicelayer.cache import get_fakeredis
from servicelayer.taskqueue import (
    Dataset,
    dataset_from_collection_id,
)


class TestDataset(TestCase):
    def setUp(self):
        self.connection = get_fakeredis()
        self.connection.flushdb()
        self.collection_id = 1

        self.dataset = Dataset(
            conn=self.connection, name=dataset_from_collection_id(self.collection_id)
        )

    def test_get_active_datasets_key(self):
        assert self.dataset.key == "tq:qdatasets"

    def test_get_active_stages_key(self):
        assert (
            self.dataset.active_stages_key
            == f"tq:qds:{self.collection_id}:active_stages"
        )

    def test_get_timestamp_keys(self):
        assert self.dataset.start_key == f"tq:qdj:{self.collection_id}:start"
        assert (
            self.dataset.last_update_key == f"tq:qdj:{self.collection_id}:last_update"
        )

    def test_tasks_per_collection_keys(self):
        assert self.dataset.finished_key == f"tq:qdj:{self.collection_id}:finished"
        assert self.dataset.running_key == f"tq:qdj:{self.collection_id}:running"
        assert self.dataset.pending_key == f"tq:qdj:{self.collection_id}:pending"
