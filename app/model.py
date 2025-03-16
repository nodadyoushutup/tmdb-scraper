from datetime import datetime
from app import db


class BaseModel(db.Model):
    __abstract__ = True

    # _id is the primary key. We'll keep it as _id everywhere.
    _id = db.Column(db.Integer, primary_key=True)
    _created_at = db.Column(db.DateTime, default=datetime.utcnow)
    _updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )
    _deleted_at = db.Column(db.DateTime, nullable=True)

    @classmethod
    def _filter_valid_data(cls, data):
        allowed_keys = {col.name for col in cls.__table__.columns}
        filtered_data = {}
        for key, value in data.items():
            if key in allowed_keys:
                filtered_data[key] = cls._handle_invalid_type(key, value)
        return filtered_data

    @classmethod
    def _handle_invalid_type(cls, key, value):
        if isinstance(value, (list, dict)):
            return None
        return value

    @classmethod
    def create(cls, data):
        filtered_data = cls._filter_valid_data(data)
        obj = cls(**filtered_data)
        db.session.add(obj)
        db.session.commit()
        return obj

    @classmethod
    def get(cls, key, value):
        return cls.query.filter(getattr(cls, key) == value).first()

    @classmethod
    def get_all(cls, key, value):
        return cls.query.filter(getattr(cls, key) == value).all()

    @classmethod
    def upsert(cls, key, data):
        if key not in data:
            raise ValueError(f"Data must include the key field: {key}")
        record = cls.get(key, data[key])
        if record:
            return record.update(data)
        else:
            return cls.create(data)

    def update(self, data):
        filtered_data = self._filter_valid_data(data)
        for key, value in filtered_data.items():
            setattr(self, key, value)
        db.session.commit()
        return self

    def delete(self):
        db.session.delete(self)
        db.session.commit()
