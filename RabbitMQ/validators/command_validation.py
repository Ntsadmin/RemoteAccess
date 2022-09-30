from pydantic import BaseModel, validator, ValidationError

SHIFT_VALIDATION = (1, 2)
RESULT_VALIDATION = (1, 2)


class OperationCommand(BaseModel):
    """
    Model to validate incoming message for operations
    """
    command_id: int
    unit_id: int
    result: int
    mode: int
    datetime: str

    @validator('result')
    def result_validator(cls, value: int):
        if value not in RESULT_VALIDATION:
            raise ValueError("Invalid result!")
        return value


class ShiftCommand(BaseModel):
    """
    Model to validate incoming message for shift
    """
    command_id: int
    shift_num: int
    datetime: str

    @validator('shift_num')
    def shift_validator(cls, value: int):
        if value not in SHIFT_VALIDATION:
            raise ValueError("Invalid shift!")
        return value
