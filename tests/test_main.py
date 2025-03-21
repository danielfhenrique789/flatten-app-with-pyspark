import sys
from unittest.mock import patch, MagicMock


def test_main_executes_job():
    sys.argv = ["main.py", "MockJob"]

    with patch("lib.main_utils.get_job_name") as mock_get_job_name, \
         patch("lib.config_manager.load_config") as mock_load_config, \
         patch("lib.config_manager.get_config") as mock_get_config, \
         patch("lib.auto_load.get_job_class") as mock_get_job_class, \
         patch("pyspark.sql.SparkSession") as mock_spark_class:

        from main import main  # Import AFTER patching

        mock_spark = MagicMock(name="MockSparkSession")

        mock_builder = MagicMock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        # This is key: SparkSession.builder is NOT callable, it's a property
        mock_spark_class.builder = mock_builder

        # Job config
        mock_get_job_name.return_value = "MockJob"
        mock_load_config.return_value = {"jobs": {"MockJob": {}}}
        mock_get_config.return_value = {"param": "value"}

        # Job class and instance
        mock_job_instance = MagicMock(name="MockJobInstance")
        mock_job_class = MagicMock(return_value=mock_job_instance)
        mock_get_job_class.return_value = mock_job_class

        # Act
        main()

        # Assert it all worked
        mock_get_job_name.assert_called_once_with(["main.py", "MockJob"])
        mock_load_config.assert_called_once()
        mock_get_config.assert_called_once()
        mock_get_job_class.assert_called_once_with("MockJob")
        mock_job_class.assert_called_once_with(mock_spark)
        mock_job_instance.run.assert_called_once_with({"param": "value"})
        mock_spark.stop.assert_called_once()