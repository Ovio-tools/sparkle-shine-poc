import os
import unittest
from unittest.mock import MagicMock, patch

import psycopg2

from database import connection


class TestGetConnection(unittest.TestCase):
    def test_uses_primary_database_url(self):
        raw_conn = MagicMock()
        with patch.dict(
            os.environ,
            {"DATABASE_URL": "postgresql://primary/db"},
            clear=True,
        ), patch.object(connection.psycopg2, "connect", return_value=raw_conn) as mock_connect:
            wrapped = connection.get_connection()

        self.assertIsInstance(wrapped, connection.Connection)
        self.assertIs(wrapped._conn, raw_conn)
        mock_connect.assert_called_once_with("postgresql://primary/db", connect_timeout=5)

    def test_falls_back_to_public_url_for_railway_internal_resolution_failures(self):
        raw_conn = MagicMock()
        error = psycopg2.OperationalError(
            'could not translate host name "postgres.railway.internal" to address'
        )
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://postgres.railway.internal/private",
                "DATABASE_PUBLIC_URL": "postgresql://public.proxy/db",
            },
            clear=True,
        ), patch.object(connection.psycopg2, "connect", side_effect=[error, raw_conn]) as mock_connect:
            wrapped = connection.get_connection()

        self.assertIs(wrapped._conn, raw_conn)
        self.assertEqual(
            mock_connect.call_args_list,
            [
                unittest.mock.call("postgresql://postgres.railway.internal/private", connect_timeout=5),
                unittest.mock.call("postgresql://public.proxy/db", connect_timeout=5),
            ],
        )

    def test_does_not_fall_back_for_non_railway_errors(self):
        error = psycopg2.OperationalError("password authentication failed")
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://localhost/dev",
                "DATABASE_PUBLIC_URL": "postgresql://public.proxy/db",
            },
            clear=True,
        ), patch.object(connection.psycopg2, "connect", side_effect=error) as mock_connect:
            with self.assertRaises(psycopg2.OperationalError):
                connection.get_connection()

        mock_connect.assert_called_once_with("postgresql://localhost/dev", connect_timeout=5)
