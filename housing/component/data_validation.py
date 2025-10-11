from housing.logger import logging
from housing.exception import HousingException
from housing.entity.config_entity import DataValidationConfig
from housing.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
import os,sys
import pandas as pd
from housing.util.util import read_yaml_file
import json

from evidently import Report
from evidently.presets import DataDriftPreset


class DataValidation:

    def __init__(
            self,
            data_validation_config: DataValidationConfig,
            data_ingestion_artifact: DataIngestionArtifact
            ) -> None:
        try:
            logging.info(f"{'>>'*30}Data Valdaition log started.{'<<'*30} \n\n")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
        except Exception as e:
            raise HousingException(e, sys) from e

    def get_train_and_test_df(self):
        try:
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)
            return train_df, test_df
        except Exception as e:
            raise HousingException(e,sys) from e

    def is_train_test_file_exists(self) -> bool:
        try:
            logging.info("Checking training and testing files are available or not")
            is_train_file_exist = False
            is_test_file_exist = False

            training_file_path = self.data_ingestion_artifact.train_file_path
            testing_file_path = self.data_ingestion_artifact.test_file_path

            is_train_file_exist = os.path.exists(training_file_path)
            is_test_file_exist = os.path.exists(testing_file_path)
        
            is_available = is_train_file_exist and is_test_file_exist

            logging.info(f"Is training and testing files exists? -> {is_available}")
            if not is_available:
                message = f"Training file: {training_file_path} or "\
                          f"Testing file: {testing_file_path} is not present"
                raise Exception(message)
            return is_available
        
        except Exception as e:
            raise HousingException(e, sys) from e
        
    def validate_dataset_schema(self) -> bool:
        try:
            validation_status = False
            schema_file_path = self.data_validation_config.schema_file_path
            testing_file_path = self.data_ingestion_artifact.test_file_path
            training_file_path = self.data_ingestion_artifact.train_file_path

            logging.info(f"Validating schema of training dataset: [{training_file_path}] and testing dataset: [{testing_file_path}]")
            schema_file_info = read_yaml_file(file_path=schema_file_path)

            is_valid_columns = False
            training_dataset = pd.read_csv(training_file_path)
            testing_dataset = pd.read_csv(testing_file_path)

            logging.info("Validating columns of Training and testing datasets")
            expected_columns = set(schema_file_info["columns"].keys())
            if (expected_columns == set(training_dataset.columns)
                    and expected_columns == set(testing_dataset.columns)):
                is_valid_columns = True
                logging.info(f"Columns of Training and testing datasets are valid? -> {is_valid_columns}")

            logging.info("Validating categorical features domain values of Training and testing datasets")
            valid_domain_values = True
            length = 0
            testing_dataset_cat_features = testing_dataset.select_dtypes(include='object').columns.to_list()
            training_dataset_cat_features = training_dataset.select_dtypes(include='object').columns.to_list()
            if len(schema_file_info["categorical_columns"]) == len(training_dataset_cat_features) == len(testing_dataset_cat_features):
                length = len(schema_file_info["categorical_columns"])
            for i in range(length):
                unique_values_in_schema = set(schema_file_info["domain_value"][schema_file_info["categorical_columns"][i]])
                unique_values_in_training_dataset = set(training_dataset[training_dataset_cat_features[i]].unique())
                unique_values_in_testing_dataset = set(testing_dataset[testing_dataset_cat_features[0]].unique())
                if unique_values_in_schema != unique_values_in_testing_dataset or unique_values_in_schema != unique_values_in_training_dataset:
                    valid_domain_values = False

            logging.info(f"domain values of categorical features in Training and testing datasets are valid? -> {valid_domain_values}")

            validation_status = is_valid_columns and valid_domain_values

            if not validation_status:
                message = f"Schema of Training file: {training_file_path} or "\
                          f"Testing file: {testing_file_path} is not valid"
                raise Exception(message)

            return validation_status
        except Exception as e:
            raise HousingException(e, sys) from e

    def get_data_drift_raw_report(self):
        try:
            train_df, test_df = self.get_train_and_test_df()
            report_obj = Report([
                            DataDriftPreset(method="psi")
                            ],
                            include_tests=True)
            base_dataset_file_path = self.data_validation_config.base_dataset_file_path
            base_dataset = pd.read_csv(base_dataset_file_path)
            train_raw_report = report_obj.run(base_dataset, train_df)
            test_raw_report = report_obj.run(base_dataset, test_df)
            return train_raw_report, test_raw_report
        
        except Exception as e:
            raise HousingException(e, sys) from e

    def save_data_drift_report(self):
        try:
            train_raw_report, test_raw_report = self.get_data_drift_raw_report()
            train_report = json.loads(train_raw_report.json())
            test_report = json.loads(test_raw_report.json())
            logging.info("Generating train_report of data drift")
            train_report_file_path = self.data_validation_config.train_report_file_path
            train_report_dir = os.path.dirname(train_report_file_path)
            os.makedirs(train_report_dir, exist_ok=True)

            with open(train_report_file_path, "w") as train_report_file:
                json.dump(train_report, train_report_file, indent=6)
            
            logging.info(f"train_Report of data drift is generated and saved at: [{train_report_file_path}]")

            logging.info("Generating test_report of data drift")
            test_report_file_path = self.data_validation_config.test_report_file_path
            test_report_dir = os.path.dirname(test_report_file_path)
            os.makedirs(test_report_dir, exist_ok=True)

            with open(test_report_file_path, "w") as test_report_file:
                json.dump(test_report, test_report_file, indent=6)
            
            logging.info(f"test_Report of data drift is generated and saved at: [{test_report_file_path}]")

            return train_report, test_report

        except Exception as e:
            raise HousingException(e, sys) from e
        
    def save_data_drift_report_page(self):
        try:
            train_raw_report, test_raw_report = self.get_data_drift_raw_report()
            logging.info("Generating webpage for train_report of data drift")
            train_report_page_file_path = self.data_validation_config.train_report_page_file_path
            train_report_page_dir = os.path.dirname(train_report_page_file_path)
            os.makedirs(train_report_page_dir, exist_ok=True)
            train_raw_report.save_html(train_report_page_file_path)

            logging.info(f"Html page for train_report of data drift is generated and saved at: [{train_report_page_file_path}]")

            logging.info("Generating webpage for test_report of data drift")
            test_report_page_file_path = self.data_validation_config.test_report_page_file_path
            test_report_page_dir = os.path.dirname(test_report_page_file_path)
            os.makedirs(test_report_page_dir, exist_ok=True)
            test_raw_report.save_html(test_report_page_file_path)

            logging.info(f"Html page for test_report of data drift is generated and saved at: [{test_report_page_file_path}]")

        except Exception as e:
            raise HousingException(e, sys) from e

    def is_data_drift_found(self) -> bool:
        try:
            is_data_drift = False
            train_raw_report, test_raw_report = self.get_data_drift_raw_report()
            train_report = train_raw_report.dict()
            train_drift_value = train_report['metrics'][0]['value']['share']
            logging.info("Checking for train_data drift")
            is_train_data_drift = False
            if train_drift_value >= 0.5:
                is_train_data_drift = True
                logging.warning(f"train_Data Drift is detected having drift value: {train_drift_value} which is >= 0.5")

            else:
                logging.info(f"train_Data Drift is not detected having drift value: {train_drift_value} which is < 0.5")

            test_report = test_raw_report.dict()
            test_drift_value = test_report['metrics'][0]['value']['share']
            logging.info("Checking for test_data drift")
            is_test_data_drift = False
            if test_drift_value >= 0.5:
                is_test_data_drift = True
                logging.warning(f"test_Data Drift is detected having drift value: {test_drift_value} which is >= 0.5")

            else:
                logging.info(f"test_Data Drift is not detected having drift value: {test_drift_value} which is < 0.5")

            is_data_drift = is_train_data_drift and is_test_data_drift

            return is_data_drift

        except Exception as e:
            raise HousingException(e, sys) from e

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            is_available = self.is_train_test_file_exists()
            validation_status = self.validate_dataset_schema()
            self.save_data_drift_report_page()
            self.save_data_drift_report()
            is_data_drift = self.is_data_drift_found()

            is_validated = is_available and validation_status and not (is_data_drift)

            if (is_validated):
                is_validated = True
                message = "Data validation is completed sucessfully"
            else:
                message = "Data validation is not completed sucessfully"

            data_validation_artifact = DataValidationArtifact(
                schema_file_path=self.data_validation_config.schema_file_path,
                train_report_file_path=self.data_validation_config.train_report_file_path,
                train_report_page_file_path=self.data_validation_config.train_report_page_file_path,
                test_report_file_path=self.data_validation_config.test_report_file_path,
                test_report_page_file_path=self.data_validation_config.test_report_page_file_path,
                is_validated=is_validated,
                message=message
                )
            logging.info(f"Data validation artifact: {data_validation_artifact}")

            return data_validation_artifact

        except Exception as e:
            raise HousingException(e, sys) from e
        
    def __del__(self):
        logging.info(f"{'>>'*30}Data Valdaition log completed.{'<<'*30} \n\n")
