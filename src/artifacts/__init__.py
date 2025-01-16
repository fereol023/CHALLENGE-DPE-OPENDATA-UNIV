import mlflow
from abc import ABC, abstractmethod

class MLFlowExp(ABC):
    def __init__(self, experiment_name):
        self.experiment_name = experiment_name
        mlflow.set_experiment(self.experiment_name)
        self.mlflow = mlflow
        print(f'-> Experiment : {self.experiment_name} started..')

    @abstractmethod
    def run(self):
        pass