from abc import abstractmethod


class UnderfolderConverter:
    @abstractmethod
    def convert(self, output_folder: str):
        raise NotImplementedError
