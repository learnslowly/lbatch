class LBatchError(Exception):
    """Base exception for user-facing lbatch errors."""


class UnsupportedFeatureError(LBatchError):
    def __init__(self, feature: str):
        super().__init__(
            f"{feature}: This sbatch feature is not supported by lbatch v1.0. "
            "Submit it directly with sbatch or open a feature request."
        )


class ParseError(LBatchError):
    pass
