import mlflow, os, yaml
from abc import ABC, abstractmethod


PROJECT_NAME = "CHALLENGE-DPE-OPENDATA-UNIV"

def get_project_root():

    current_path = os.getcwd()
    path_parts = current_path.split(os.sep)
    project_root_index = path_parts.index(PROJECT_NAME)
    return os.sep.join(path_parts[:project_root_index + 1])


class MLFlowExp(ABC):
    def __init__(self, experiment_name):
        self.experiment_name = experiment_name
        mlflow.create_experiment(self.experiment_name, artifact_location=os.path.join('file:/', get_project_root(), 'mlruns'))
        mlflow.set_experiment(self.experiment_name)
        mlflow.set_tracking_uri(os.path.join(get_project_root(), 'mlruns'))
        mlflow.set_registry_uri(os.path.join('file:/', get_project_root(), 'mlruns'))
        self.mlflow = mlflow
        # self.mlflow.autolog()
        print(f'-> Experiment : {self.experiment_name} started..')
        print(f'-> Tracking folder : {self.mlflow.get_tracking_uri()}')
        print(f'-> Registry folder : {self.mlflow.get_registry_uri()}')

    @abstractmethod
    def run(self):
        pass

    def get_exp_class_folder_path(self):
        f_ = os.path.abspath(__file__)
        return os.path.dirname(f_)
    
    def get_root_folder_path(self):
        current_folder = self.get_exp_class_folder_path()
        return os.path.abspath(os.path.join(current_folder, '..', '..'))