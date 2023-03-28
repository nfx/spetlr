from typing import List, Protocol, Union

from spetlr.etl import EtlBase, Orchestrator
from spetlr.etl.loaders import SimpleLoader
from spetlr.transformers import UnionTransformerNC

from .log_transformer import LogTransformer


class Handle(Protocol):
    def append(self):
        ...


class LoggingOrchestrator(Orchestrator):
    def __init__(
        self,
        log_handle: Union[Handle, List[Handle]],
        suppress_composition_warning=False,
    ):
        if isinstance(log_handle, type):
            raise Exception("'log_handle' need to be an instance of a class")
        self.log_handle = log_handle
        super().__init__(suppress_composition_warning)

    def log_step(
        self,
        log_transformers: Union[LogTransformer, List[LogTransformer]],
    ) -> "LoggingOrchestrator":
        if not isinstance(log_transformers, List):
            log_transformers = [log_transformers]

        output_keys = []

        for transformer in log_transformers:
            self.steps.append(transformer)
            output_keys.append(transformer.dataset_output_key)

        if len(output_keys) == 1:
            key = output_keys[0]

        elif len(output_keys) > 1:
            key = "UnionTransformerNC"

            self.steps.append(
                UnionTransformerNC(
                    dataset_input_keys=output_keys,
                    dataset_output_key=key,
                )
            )

        if not isinstance(self.log_handle, List):
            self.log_handle = [self.log_handle]

        for handle in self.log_handle:
            self.steps.append(
                SimpleLoader(
                    handle=handle,
                    mode="append",
                    dataset_input_keys=key,
                )
            )

        return self

    log_with = log_step

    def step(self, etl: EtlBase) -> "LoggingOrchestrator":
        return super().step(etl)

    extract_from = step
    transform_with = step
    load_into = step
