import json
from sqlalchemy import Column, Integer, String, ForeignKey, PickleType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class ParameterType(Base):
    __tablename__ = 'parameter_type'
    id = Column(Integer, primary_key=True)
    value = Column(String(20), nullable=False, unique=True)


class ValueEncoding(Base):
    __tablename__ = 'value_encoding'
    id = Column(Integer, primary_key=True)
    value = Column(String(20), nullable=False, unique=True)


class CodeSet(Base):
    __tablename__ = 'code_set'
    id = Column(Integer, primary_key=True)
    value = Column(String(250), nullable=False)


class Unit(Base):
    __tablename__ = 'unit'
    id = Column(Integer, primary_key=True)
    value = Column(String(250), nullable=False, unique=True)


class FillValue(Base):
    __tablename__ = 'fill_value'
    id = Column(Integer, primary_key=True)
    value = Column(String(20), nullable=False)


class FunctionType(Base):
    __tablename__ = 'function_type'
    id = Column(Integer, primary_key=True)
    value = Column(String(250), nullable=False, unique=True)


class ParameterFunction(Base):
    __tablename__ = 'parameter_function'
    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    function_type_id = Column(Integer, ForeignKey('function_type.id'))
    function_type = relationship(FunctionType)
    function = Column(String(250))
    owner = Column(String(250))
    description = Column(String(4096))
    qc_flag = db.Column(db.String(32))


class Parameter(Base):
    __tablename__ = 'parameter'
    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    parameter_type_id = Column(Integer, ForeignKey('parameter_type.id'))
    parameter_type = relationship(ParameterType)
    value_encoding_id = Column(Integer, ForeignKey('value_encoding.id'))
    value_encoding = relationship(ValueEncoding)
    code_set_id = Column(Integer, ForeignKey('code_set.id'))
    code_set = relationship(CodeSet)
    unit_id = Column(Integer, ForeignKey('unit.id'))
    unit = relationship(Unit)
    fill_value_id = Column(Integer, ForeignKey('fill_value.id'))
    fill_value = relationship(FillValue)
    display_name = Column(String(4096))
    precision = Column(Integer)
    parameter_function_id = Column(Integer, ForeignKey('parameter_function.id'))
    parameter_function = relationship(ParameterFunction)
    parameter_function_map = Column(PickleType(pickler=json))
    data_product_identifier = Column(String(250))
    description = Column(String(4096))
    streams = relationship('Stream', secondary='stream_parameter')

    def parse_pdid(self, pdid_string):
        return int(pdid_string.split()[0][2:])

    def needs(self):
        needed = set()
        if self.parameter_type.value == 'function':
            for value in self.parameter_function_map.values():
                if value.startswith('PD'):
                    try:
                        pdid = self.parse_pdid(value)
                        sub_param = Parameter.query.get(pdid)
                        needed = needed.union(sub_param.needs())
                    except ValueError:
                        needed.add('MISSING: ' + value)

        needed.add(self)
        return needed

    def needs_cc(self):
        needed = set()
        if self.parameter_type.value == 'function':
            for value in self.parameter_function_map.values():
                if value.startswith('PD'):
                    try:
                        pdid = self.parse_pdid(value)
                        sub_param = Parameter.query.get(pdid)
                        needed = needed.union(sub_param.needs_cc())
                    except ValueError:
                        pass

                if value.startswith('CC'):
                    needed.add(value)

        return needed

    def asdict(self):
        return {
            'pd_id': 'PD%d' % self.id,
            'name': self.name,
            'type': self.parameter_type.value if self.parameter_type is not None else None,
            'unit': self.unit.value if self.unit is not None else None,
            'fill': self.fill_value.value if self.fill_value is not None else None,
            'encoding': self.value_encoding.value if self.value_encoding is not None else None,
            'precision': self.precision
        }


class StreamParameter(Base):
    __tablename__ = 'stream_parameter'
    stream_id = Column(Integer, ForeignKey('stream.id'), primary_key=True)
    parameter_id = Column(Integer, ForeignKey('parameter.id'), primary_key=True)


class Stream(Base):
    __tablename__ = 'stream'
    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False, unique=True)
    parameters = relationship('Parameter', secondary='stream_parameter')
