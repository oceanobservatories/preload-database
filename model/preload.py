import json
from sqlalchemy import Column, Integer, String, ForeignKey, PickleType, Table
from sqlalchemy.orm import relationship
try:
    # Needed for 'parse_preload.py'
    from database import Base
except ImportError:
    # Needed for Stream Engine
    from ..database import Base


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
    qc_flag = Column(String(32))


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
    standard_name = Column(String(4096))
    precision = Column(Integer)
    parameter_function_id = Column(Integer, ForeignKey('parameter_function.id'))
    parameter_function = relationship(ParameterFunction)
    parameter_function_map = Column(PickleType(pickler=json))
    data_product_identifier = Column(String(250))
    description = Column(String(4096))
    streams = relationship('Stream', secondary='stream_parameter')

    def parse_pdid(self, pdid_string):
        return int(pdid_string.split()[0][2:])

    def needs(self, needed=None):
        if needed is None:
            needed = []

        if self in needed:
            return

        if self.parameter_type.value == 'function':
            for value in self.parameter_function_map.values():
                if isinstance(value,basestring) and value.startswith('PD'):
                    try:
                        pdid = self.parse_pdid(value)
                        sub_param = Parameter.query.get(pdid)
                        if sub_param in needed:
                            continue
                        sub_param.needs(needed)
                    except (ValueError, AttributeError):
                        pass

        if self not in needed:
            needed.append(self)
        return needed

    def needs_cc(self, needed=None):
        if needed is None:
            needed = []

        if self.parameter_type.value == 'function':
            for value in self.parameter_function_map.values():
                if isinstance(value,basestring) and value.startswith('CC') and value not in needed:
                    needed.append(value)

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

class StreamDependency(Base):
    __tablename__ = 'stream_dependency'
    source_stream_id = Column(Integer, ForeignKey("stream.id"), primary_key=True)
    product_stream_id = Column(Integer, ForeignKey("stream.id"), primary_key=True)

class Stream(Base):
    __tablename__ = 'stream'
    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False, unique=True)
    time_parameter = Column(Integer, default=7)
    parameters = relationship('Parameter', secondary='stream_parameter')
    binsize = Column(Integer, nullable=False)
    source_streams = relationship('Stream',
                                secondary="stream_dependency",
                                primaryjoin=id==StreamDependency.product_stream_id,
                                secondaryjoin=id==StreamDependency.source_stream_id)

    product_streams = relationship('Stream',
                                secondary="stream_dependency",
                                primaryjoin=id==StreamDependency.source_stream_id,
                                secondaryjoin=id==StreamDependency.product_stream_id)
