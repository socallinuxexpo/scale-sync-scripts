"""Tests for scale_email_sync.py"""

import json
import pytest
import yaml
from unittest.mock import Mock, MagicMock, patch, mock_open, call
from io import StringIO

import sys
import os

# Add parent directory to path so we can import the module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scale_email_sync import load_config, RegData, ListMonk


class TestLoadConfig:
    """Tests for load_config function"""

    def test_load_config_valid_yaml(self):
        """Test loading a valid YAML configuration file"""
        yaml_content = """
listmonk:
  api_url: https://test.example.com/api
  api_key: test_key_123
regdb:
  host: localhost
  user: testuser
  password: testpass
  database: testdb
datadog:
  api_key: dd_test_key
"""
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            config = load_config("test_config.yml")

        assert config["listmonk"]["api_url"] == "https://test.example.com/api"
        assert config["listmonk"]["api_key"] == "test_key_123"
        assert config["regdb"]["host"] == "localhost"
        assert config["datadog"]["api_key"] == "dd_test_key"

    def test_load_config_file_not_found(self):
        """Test that FileNotFoundError is raised for missing file"""
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent_file.yml")


class TestRegData:
    """Tests for RegData class"""

    @pytest.fixture
    def mock_config(self):
        return {
            "regdb": {
                "host": "localhost",
                "user": "testuser",
                "password": "testpass",
                "database": "testdb",
            }
        }

    @pytest.fixture
    def reg_data(self, mock_config):
        return RegData(mock_config)

    def test_fetch_file_data_http(self, reg_data):
        """Test fetching CSV data from HTTP URL"""
        mock_response = Mock()
        mock_response.text = "id,email,can_email\n1,test@example.com,1"
        mock_response.raise_for_status = Mock()

        with patch("requests.get", return_value=mock_response) as mock_get:
            result = reg_data._fetch_file_data("http://example.com/data.csv")

        mock_get.assert_called_once_with("http://example.com/data.csv")
        assert result == "id,email,can_email\n1,test@example.com,1"

    def test_fetch_file_data_file(self, reg_data):
        """Test fetching CSV data from local file"""
        csv_content = "id,email,can_email\n1,test@example.com,1"

        with patch("builtins.open", mock_open(read_data=csv_content)):
            result = reg_data._fetch_file_data("/path/to/file.csv")

        assert result == csv_content

    def test_get_csv_data(self, reg_data):
        """Test parsing CSV data into subscriber dictionary"""
        csv_content = "id,email,can_email\n1,test@example.com,1\n2,USER@EXAMPLE.COM,0\n3,admin@example.com,2"

        with patch.object(
            reg_data, "_fetch_file_data", return_value=csv_content
        ):
            subscribers = reg_data.get_csv_data("test.csv")

        assert len(subscribers) == 3
        assert "test@example.com" in subscribers
        assert subscribers["test@example.com"]["id"] == "1"
        assert subscribers["test@example.com"]["can_email"] == 1
        # Test email lowercasing
        assert "user@example.com" in subscribers
        assert subscribers["user@example.com"]["can_email"] == 0
        assert subscribers["admin@example.com"]["can_email"] == 2

    @patch("MySQLdb.connect")
    def test_get_db_data(self, mock_connect, reg_data):
        """Test fetching data from MySQL database"""
        # Setup mock database connection
        mock_db = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_db
        mock_db.cursor.return_value = mock_cursor

        # Mock database results
        mock_cursor.fetchall.return_value = [
            ("test@example.com", "John Doe", 1),
            ("ADMIN@EXAMPLE.COM", "Jane Admin", 2),
        ]

        subscribers = reg_data.get_db_data()

        # Verify database connection
        mock_connect.assert_called_once_with(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )

        # Verify query execution
        mock_cursor.execute.assert_called_once()

        # Verify returned data
        assert len(subscribers) == 2
        assert "test@example.com" in subscribers
        assert subscribers["test@example.com"]["name"] == "John Doe"
        assert subscribers["test@example.com"]["can_email"] == 1
        # Test email lowercasing
        assert "admin@example.com" in subscribers


class TestListMonk:
    """Tests for ListMonk class"""

    @pytest.fixture
    def mock_config(self):
        return {
            "listmonk": {
                "api_url": "https://test.example.com/api",
                "api_key": "test_key",
            },
            "datadog": {
                "api_key": "dd_test_key",
            },
        }

    @pytest.fixture
    def listmonk(self, mock_config):
        return ListMonk(mock_config, dry_run=False, remove=False, prod=False)

    @pytest.fixture
    def listmonk_dry_run(self, mock_config):
        return ListMonk(mock_config, dry_run=True, remove=False, prod=False)

    @pytest.fixture
    def listmonk_with_remove(self, mock_config):
        return ListMonk(mock_config, dry_run=False, remove=True, prod=False)

    def test_init_test_lists(self, listmonk):
        """Test initialization with test list IDs"""
        assert listmonk.list_ids == ListMonk.TEST_LIST_IDS
        assert listmonk.dry_run is False
        assert listmonk.remove is False

    def test_init_prod_lists(self, mock_config):
        """Test initialization with production list IDs"""
        lm = ListMonk(mock_config, dry_run=False, remove=False, prod=True)
        assert lm.list_ids == ListMonk.PROD_LIST_IDS

    def test_init_stats_tracking(self, listmonk):
        """Test that stats are properly initialized"""
        assert "adds" in listmonk.stats
        assert "removes" in listmonk.stats
        for list_name in listmonk.list_ids.keys():
            assert list_name in listmonk.stats["adds"]
            assert list_name in listmonk.stats["removes"]
            assert listmonk.stats["adds"][list_name] == 0
            assert listmonk.stats["removes"][list_name] == 0

    @patch("requests.get")
    def test_get_without_pagination(self, mock_get, listmonk):
        """Test _get method without pagination"""
        mock_response = Mock()
        mock_response.text = json.dumps({"data": {"results": [{"id": 1}]}})
        mock_get.return_value = mock_response

        result = listmonk._get("https://test.example.com/api/test", {})

        assert result["data"]["results"] == [{"id": 1}]

    @patch("requests.get")
    def test_get_with_pagination(self, mock_get, listmonk):
        """Test _get method with pagination"""
        # First page response
        mock_response_1 = Mock()
        mock_response_1.text = json.dumps(
            {"data": {"results": [{"id": 1}, {"id": 2}], "total": 3}}
        )

        # Second page response
        mock_response_2 = Mock()
        mock_response_2.text = json.dumps(
            {"data": {"results": [{"id": 3}], "total": 3}}
        )

        mock_get.side_effect = [mock_response_1, mock_response_2]

        result = listmonk._get("https://test.example.com/api/test", {})

        # Should have made 2 requests
        assert mock_get.call_count == 2
        # Should have combined results
        assert len(result["data"]["results"]) == 3
        assert result["data"]["results"] == [{"id": 1}, {"id": 2}, {"id": 3}]

    @patch("requests.post")
    def test_post(self, mock_post, listmonk):
        """Test _post method"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        data = {"email": "test@example.com"}
        response = listmonk._post("https://test.example.com/api/test", data)

        mock_post.assert_called_once()
        assert response.status_code == 200

    @patch("requests.put")
    def test_put(self, mock_put, listmonk):
        """Test _put method"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_put.return_value = mock_response

        data = {"id": 1}
        response = listmonk._put("https://test.example.com/api/test", data)

        mock_put.assert_called_once()
        assert response.status_code == 200

    @patch.object(ListMonk, "_get")
    def test_get_all_subscribers(self, mock_get, listmonk):
        """Test get_all_subscribers method"""
        mock_get.return_value = {
            "data": {
                "results": [
                    {"id": 1, "email": "user1@example.com"},
                    {"id": 2, "email": "user2@example.com"},
                ]
            }
        }

        subscribers = listmonk.get_all_subscribers()

        assert len(subscribers) == 2
        assert subscribers[0]["email"] == "user1@example.com"

    def test_list_ids_to_names(self, listmonk):
        """Test converting list IDs to names"""
        result = listmonk.list_ids_to_names([14, 18])
        assert "announce" in result
        assert "logistics" in result

    def test_get_expected_lists(self, listmonk):
        """Test get_expected_lists based on can_email level"""
        # Level -1: no lists
        assert listmonk.get_expected_lists({"can_email": -1}) == []

        # Level 0: logistics only
        expected = listmonk.get_expected_lists({"can_email": 0})
        assert len(expected) == 1
        assert listmonk.list_ids["logistics"] in expected

        # Level 1: logistics and announce
        expected = listmonk.get_expected_lists({"can_email": 1})
        assert len(expected) == 2
        assert listmonk.list_ids["logistics"] in expected
        assert listmonk.list_ids["announce"] in expected

        # Level 2: all three lists
        expected = listmonk.get_expected_lists({"can_email": 2})
        assert len(expected) == 3
        assert listmonk.list_ids["logistics"] in expected
        assert listmonk.list_ids["announce"] in expected
        assert listmonk.list_ids["sponsors"] in expected

    def test_get_missing_lists(self, listmonk):
        """Test identifying missing lists for a subscriber"""
        subscriber = {
            "lists": [
                {"id": listmonk.list_ids["announce"]},
            ]
        }
        expected_lists = [
            listmonk.list_ids["announce"],
            listmonk.list_ids["logistics"],
        ]

        missing = listmonk.get_missing_lists(subscriber, expected_lists)
        assert len(missing) == 1
        assert listmonk.list_ids["logistics"] in missing

    def test_get_extra_lists(self, listmonk):
        """Test identifying extra lists for a subscriber"""
        subscriber = {
            "lists": [
                {"id": listmonk.list_ids["announce"]},
                {"id": listmonk.list_ids["sponsors"]},
            ]
        }
        expected_lists = [listmonk.list_ids["announce"]]

        extra = listmonk.get_extra_lists(subscriber, expected_lists)
        assert len(extra) == 1
        assert listmonk.list_ids["sponsors"] in extra

    @patch.object(ListMonk, "_post")
    @patch.object(ListMonk, "_get")
    def test_add_subscriber_new(self, mock_get, mock_post, listmonk):
        """Test adding a new subscriber"""
        # Subscriber doesn't exist
        mock_get.return_value = {"data": {"results": []}}

        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response

        lists = [listmonk.list_ids["announce"]]
        listmonk.add_subscriber("new@example.com", lists)

        # Verify POST was called
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "new@example.com" in str(call_args)

        # Verify stats were updated
        assert listmonk.stats["adds"]["announce"] == 1

    @patch.object(ListMonk, "_get")
    @patch.object(ListMonk, "add_subscriber_to_lists")
    def test_add_subscriber_existing(
        self, mock_add_to_lists, mock_get, listmonk
    ):
        """Test adding an existing subscriber to lists"""
        existing_subscriber = {"id": 1, "email": "existing@example.com"}
        mock_get.return_value = {"data": {"results": [existing_subscriber]}}

        lists = [listmonk.list_ids["announce"]]
        listmonk.add_subscriber("existing@example.com", lists)

        # Should call add_subscriber_to_lists instead of creating new
        mock_add_to_lists.assert_called_once_with(existing_subscriber, lists)

    @patch.object(ListMonk, "_post")
    @patch.object(ListMonk, "_get")
    def test_add_subscriber_dry_run(
        self, mock_get, mock_post, listmonk_dry_run
    ):
        """Test adding a subscriber in dry-run mode"""
        mock_get.return_value = {"data": {"results": []}}

        lists = [listmonk_dry_run.list_ids["announce"]]
        listmonk_dry_run.add_subscriber("test@example.com", lists)

        # Should NOT call POST in dry-run mode
        mock_post.assert_not_called()

        # But should still track stats
        assert listmonk_dry_run.stats["adds"]["announce"] == 1

    @patch.object(ListMonk, "_put")
    def test_add_subscriber_to_lists(self, mock_put, listmonk):
        """Test adding existing subscriber to lists"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_put.return_value = mock_response

        subscriber = {"id": 1, "email": "test@example.com"}
        lists = [listmonk.list_ids["announce"], listmonk.list_ids["logistics"]]

        listmonk.add_subscriber_to_lists(subscriber, lists)

        mock_put.assert_called_once()

        # Verify stats were updated
        assert listmonk.stats["adds"]["announce"] == 1
        assert listmonk.stats["adds"]["logistics"] == 1

    @patch.object(ListMonk, "_put")
    def test_add_subscriber_to_lists_dry_run(self, mock_put, listmonk_dry_run):
        """Test adding subscriber to lists in dry-run mode"""
        subscriber = {"id": 1, "email": "test@example.com"}
        lists = [listmonk_dry_run.list_ids["announce"]]

        listmonk_dry_run.add_subscriber_to_lists(subscriber, lists)

        # Should NOT call PUT in dry-run mode
        mock_put.assert_not_called()

        # But should still track stats
        assert listmonk_dry_run.stats["adds"]["announce"] == 1

    @patch.object(ListMonk, "_put")
    def test_remove_subscriber_from_lists(self, mock_put, listmonk_with_remove):
        """Test removing subscriber from lists"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_put.return_value = mock_response

        subscriber = {"id": 1, "email": "test@example.com"}
        lists = [listmonk_with_remove.list_ids["sponsors"]]

        listmonk_with_remove.remove_subscriber_from_lists(subscriber, lists)

        mock_put.assert_called_once()

        # Verify stats were updated
        assert listmonk_with_remove.stats["removes"]["sponsors"] == 1

    @patch.object(ListMonk, "_put")
    def test_remove_subscriber_from_lists_dry_run(self, mock_put, mock_config):
        """Test removing subscriber from lists in dry-run mode"""
        lm = ListMonk(mock_config, dry_run=True, remove=True, prod=False)

        subscriber = {"id": 1, "email": "test@example.com"}
        lists = [lm.list_ids["sponsors"]]

        lm.remove_subscriber_from_lists(subscriber, lists)

        # Should NOT call PUT in dry-run mode
        mock_put.assert_not_called()

        # But should still track stats
        assert lm.stats["removes"]["sponsors"] == 1

    @patch.object(ListMonk, "get_all_subscribers")
    @patch.object(ListMonk, "add_subscriber")
    @patch.object(ListMonk, "add_subscriber_to_lists")
    @patch.object(ListMonk, "remove_subscriber_from_lists")
    def test_sync_list_add_new_subscriber(
        self, mock_remove, mock_add_to_lists, mock_add, mock_get_all, listmonk
    ):
        """Test sync_list adds new subscribers"""
        # No existing subscribers
        mock_get_all.return_value = []

        # New subscriber to add
        updated_subscribers = {
            "new@example.com": {
                "email": "new@example.com",
                "can_email": 1,
            }
        }

        listmonk.sync_list(updated_subscribers)

        # Should call add_subscriber for the new user
        mock_add.assert_called_once()

    @patch.object(ListMonk, "get_all_subscribers")
    @patch.object(ListMonk, "add_subscriber_to_lists")
    def test_sync_list_update_existing_subscriber(
        self, mock_add_to_lists, mock_get_all, listmonk
    ):
        """Test sync_list updates existing subscribers with missing lists"""
        # Existing subscriber with only logistics list
        mock_get_all.return_value = [
            {
                "id": 1,
                "email": "existing@example.com",
                "lists": [{"id": listmonk.list_ids["logistics"]}],
            }
        ]

        # Updated data shows they should also have announce list
        updated_subscribers = {
            "existing@example.com": {
                "email": "existing@example.com",
                "can_email": 1,  # Should have logistics + announce
            }
        }

        listmonk.sync_list(updated_subscribers)

        # Should add missing announce list
        mock_add_to_lists.assert_called_once()
        call_args = mock_add_to_lists.call_args
        assert listmonk.list_ids["announce"] in call_args[0][1]

    @patch.object(ListMonk, "get_all_subscribers")
    @patch.object(ListMonk, "remove_subscriber_from_lists")
    def test_sync_list_remove_extra_lists(
        self, mock_remove, mock_get_all, listmonk_with_remove
    ):
        """Test sync_list removes subscribers from extra lists"""
        # Existing subscriber with all lists
        mock_get_all.return_value = [
            {
                "id": 1,
                "email": "existing@example.com",
                "lists": [
                    {"id": listmonk_with_remove.list_ids["announce"]},
                    {"id": listmonk_with_remove.list_ids["logistics"]},
                    {"id": listmonk_with_remove.list_ids["sponsors"]},
                ],
            }
        ]

        # Updated data shows they should only have logistics
        updated_subscribers = {
            "existing@example.com": {
                "email": "existing@example.com",
                "can_email": 0,  # Should only have logistics
            }
        }

        listmonk_with_remove.sync_list(updated_subscribers)

        # Should remove extra lists
        mock_remove.assert_called_once()

    @patch.object(ListMonk, "get_all_subscribers")
    @patch.object(ListMonk, "remove_subscriber_from_lists")
    def test_sync_list_remove_not_in_csv(
        self, mock_remove, mock_get_all, listmonk_with_remove
    ):
        """Test sync_list removes subscribers not in CSV when remove flag is set"""
        # Existing subscriber not in updated list
        mock_get_all.return_value = [
            {
                "id": 1,
                "email": "removed@example.com",
                "lists": [{"id": listmonk_with_remove.list_ids["announce"]}],
            }
        ]

        # Empty updated subscribers
        updated_subscribers = {}

        listmonk_with_remove.sync_list(updated_subscribers)

        # Should remove from all lists
        mock_remove.assert_called_once()

    @patch.object(ListMonk, "get_all_subscribers")
    @patch.object(ListMonk, "remove_subscriber_from_lists")
    def test_sync_list_no_remove_flag(
        self, mock_remove, mock_get_all, listmonk
    ):
        """Test sync_list doesn't remove when remove flag is not set"""
        # Note: listmonk fixture has remove=False
        mock_get_all.return_value = [
            {
                "id": 1,
                "email": "existing@example.com",
                "lists": [{"id": listmonk.list_ids["announce"]}],
            }
        ]

        # Empty updated subscribers
        updated_subscribers = {}

        listmonk.sync_list(updated_subscribers)

        # Should NOT remove
        mock_remove.assert_not_called()

    @patch("time.time")
    @patch("scale_email_sync.ApiClient")
    @patch("scale_email_sync.MetricsApi")
    def test_report_stats_to_datadog(
        self, mock_metrics_api, mock_api_client, mock_time, listmonk
    ):
        """Test reporting stats to Datadog"""
        mock_time.return_value = 1234567890

        # Set some stats
        listmonk.stats["adds"]["announce"] = 5
        listmonk.stats["removes"]["announce"] = 2

        # Mock API client context manager
        mock_client_instance = MagicMock()
        mock_api_client.return_value.__enter__.return_value = (
            mock_client_instance
        )

        # Mock MetricsApi instance
        mock_api_instance = MagicMock()
        mock_metrics_api.return_value = mock_api_instance

        listmonk.report_stats_to_datadog()

        # Verify submit_metrics was called
        mock_api_instance.submit_metrics.assert_called()

    def test_report_stats_to_datadog_dry_run(self, listmonk_dry_run):
        """Test reporting stats in dry-run mode doesn't send to Datadog"""
        listmonk_dry_run.stats["adds"]["announce"] = 5

        with patch("scale_email_sync.ApiClient") as mock_api_client:
            listmonk_dry_run.report_stats_to_datadog()

            # Should NOT initialize Datadog client in dry-run
            mock_api_client.assert_not_called()

    def test_report_stats_to_datadog_no_api_key(self, mock_config):
        """Test reporting stats without Datadog API key"""
        # Remove datadog config
        config_without_dd = mock_config.copy()
        config_without_dd["datadog"] = {}

        lm = ListMonk(
            config_without_dd, dry_run=False, remove=False, prod=False
        )

        with patch("scale_email_sync.ApiClient") as mock_api_client:
            lm.report_stats_to_datadog()

            # Should NOT initialize Datadog client without API key
            mock_api_client.assert_not_called()


class TestIntegration:
    """Integration tests for the full workflow"""

    @pytest.fixture
    def mock_config(self):
        return {
            "listmonk": {
                "api_url": "https://test.example.com/api",
                "api_key": "test_key",
            },
            "regdb": {
                "host": "localhost",
                "user": "testuser",
                "password": "testpass",
                "database": "testdb",
            },
            "datadog": {
                "api_key": "dd_test_key",
            },
        }

    @patch.object(ListMonk, "get_all_subscribers")
    @patch.object(ListMonk, "add_subscriber")
    @patch.object(RegData, "get_csv_data")
    def test_full_sync_workflow(
        self, mock_get_csv, mock_add_subscriber, mock_get_all, mock_config
    ):
        """Test full synchronization workflow"""
        # Setup CSV data
        mock_get_csv.return_value = {
            "new@example.com": {
                "email": "new@example.com",
                "can_email": 1,
            }
        }

        # No existing subscribers
        mock_get_all.return_value = []

        # Create instances
        reg_data = RegData(mock_config)
        listmonk = ListMonk(
            mock_config, dry_run=False, remove=False, prod=False
        )

        # Run sync
        subscribers = reg_data.get_csv_data("test.csv")
        listmonk.sync_list(subscribers)

        # Verify new subscriber was added
        mock_add_subscriber.assert_called_once()
