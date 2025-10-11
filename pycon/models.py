import msgspec


class TemperatureReading(msgspec.Struct):
    sensor_id: str
    timestamp: float  # Unix timestamp
    temperature_celsius: float


class RegistrationMessage(msgspec.Struct, tag=True):
    sensor_id: str
    sensor_type: list[str]
    location: tuple[float, float]  # (latitude, longitude)


class SensorReading(msgspec.Struct, tag=True):
    temperature_reading: TemperatureReading | None


class Message(msgspec.Struct):
    kind: str  # 'registration' or 'reading'
    payload: RegistrationMessage | SensorReading

    sequence_src: str
    sequence_nr: int
