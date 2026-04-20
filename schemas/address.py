from pydantic import BaseModel


class CreateAddress(BaseModel):
    first_name: str
    last_name: str
    city: str
    state: str
    pincode: int
    phone_number: int
    date_of_birth: str
    title: str
    address: str


class UpdateAddress(BaseModel):
    id: int
    first_name: str
    last_name: str
    city: str
    state: str
    pincode: int
    phone_number: int
    date_of_birth: str
    title: str
    address: str


class DeleteAddress(BaseModel):
    id: int
