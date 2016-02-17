import json
from numbers import Number
from pprint import pformat

from sqlalchemy import Column, Integer, String, ForeignKey, PickleType, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


class UnfulfilledParameterException(Exception):
    pass


Base = declarative_base()


def _resolve_or_none(reference):
    if reference is not None:
        return reference.value


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
    _function_type = relationship(FunctionType)
    function = Column(String(250))
    owner = Column(String(250))
    description = Column(String(4096))
    qc_flag = Column(String(32))

    @property
    def function_type(self):
        return _resolve_or_none(self._function_type)

    def __repr__(self):
        return 'ParameterFunction(id: %d type: %r owner: %r function: %r' % (self.id, self.function_type,
                                                                             self.owner, self.function)


class Parameter(Base):
    __tablename__ = 'parameter'
    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    parameter_type_id = Column(Integer, ForeignKey('parameter_type.id'))
    _parameter_type = relationship(ParameterType)
    value_encoding_id = Column(Integer, ForeignKey('value_encoding.id'))
    _value_encoding = relationship(ValueEncoding)
    code_set_id = Column(Integer, ForeignKey('code_set.id'))
    _code_set = relationship(CodeSet)
    unit_id = Column(Integer, ForeignKey('unit.id'))
    _unit = relationship(Unit)
    fill_value_id = Column(Integer, ForeignKey('fill_value.id'))
    _fill_value = relationship(FillValue)
    display_name = Column(String(4096))
    standard_name = Column(String(4096))
    precision = Column(Integer)
    parameter_function_id = Column(Integer, ForeignKey('parameter_function.id'))
    parameter_function = relationship(ParameterFunction)
    parameter_function_map = Column(PickleType(pickler=json))
    data_product_identifier = Column(String(250))
    description = Column(String(4096))
    streams = relationship('Stream', secondary='stream_parameter')

    @property
    def attrs(self):
        long_name = self.display_name if self.display_name is not None else self.name
        attrs = {
            'units': self.unit,
            '_FillValue': self.fill_value,
            'long_name': long_name,
            'standard_name': self.standard_name,
            'comment': self.description,
            'data_product_identifier': self.data_product_identifier,
        }
        return {k: v for k,v in attrs.iteritems() if v is not None}

    @property
    def is_function(self):
        return bool(self.parameter_function)

    @property
    def parameter_type(self):
        return _resolve_or_none(self._parameter_type)

    @property
    def value_encoding(self):
        return _resolve_or_none(self._value_encoding)

    @property
    def code_set(self):
        return _resolve_or_none(self._code_set)

    @property
    def unit(self):
        return _resolve_or_none(self._unit)

    @property
    def fill_value(self):
        return _resolve_or_none(self._fill_value)

    @property
    def is_l1(self):
        return self.is_function and not any((p.is_function for source, params in self.needs for p in params))

    @property
    def is_l2(self):
        return self.is_function and not self.is_l1

    @staticmethod
    def parse_pdid(pdid_string):
        return int(pdid_string.split()[0][2:])

    @property
    def needs(self):
        """
        Calculate all parameters needed to generate this parameter 1 level deep in the dependency graph
        :return: List of (Stream, (params,)) tuples
        """
        needed = []
        if self.is_function:
            for value in self.parameter_function_map.values():
                # value is number, skip
                if not isinstance(value, basestring):
                    continue
                # value is a specific parameter, store as (None, (param))
                if value.startswith('PD'):
                    param = (Parameter.query.get(self.parse_pdid(value)),)
                    needed.append((None, param))
                # value is a data product identifier, store as (None, (p1, p2))
                elif value.startswith('dpi_'):
                    _, dpi = value.split('_', 1)
                    needed.append((None,
                                   tuple(Parameter.query.filter(Parameter.data_product_identifier == dpi).all())))
                # value is a calibration coefficient
                elif value.startswith('CC'):
                    continue
                # value may be STREAM.PARAMETER, store as (stream, (param,))
                else:
                    if '.' in value:
                        stream, parameter = value.split('.', 1)
                        stream = Stream.query.filter(Stream.name == stream).first()
                        parameter = Parameter.query.get(self.parse_pdid(parameter))
                        if stream and parameter:
                            needed.append((stream, (parameter,)))
        return needed

    @property
    def needs_map(self):
        """
        Calculate all parameters needed to generate this parameter 1 level deep in the dependency graph
        :return:
        """
        needed = {}
        if self.is_function:
            for name, value in self.parameter_function_map.iteritems():
                # value is number, skip
                if isinstance(value, Number):
                    needed[name] = (None, value)
                elif isinstance(value, basestring):
                    stream = None
                    params = None
                    # value is a specific parameter, store as (None, (param))
                    if value.startswith('PD'):
                        params = (Parameter.query.get(self.parse_pdid(value)),)
                    # value is a data product identifier, store as (None, (p1, p2))
                    elif value.startswith('dpi_'):
                        _, dpi = value.split('_', 1)
                        params = tuple(Parameter.query.filter(Parameter.data_product_identifier == dpi).all())
                    # value is a calibration coefficient
                    elif value.startswith('CC'):
                        params = value
                    # value may be STREAM.PARAMETER, store as (stream, (param,))
                    else:
                        if '.' in value:
                            stream, parameter = value.split('.', 1)
                            stream = Stream.query.filter(Stream.name == stream).first()
                            params = (Parameter.query.get(self.parse_pdid(parameter)),)
                    if params:
                        needed[name] = (stream, params)
        return needed

    @property
    def needs_cc(self):
        if self.is_function:
            return [value
                    for value in self.parameter_function_map.values()
                    if isinstance(value, basestring) and value.startswith('CC')]
        return []

    def asdict(self):
        return {
            'pd_id': 'PD%d' % self.id,
            'name': self.name,
            'type': self.parameter_type,
            'unit': self.unit,
            'fill': self.fill_value,
            'encoding': self.value_encoding,
            'precision': self.precision
        }

    def __repr__(self):
        return 'Parameter({:d}: {:s})'.format(self.id, self.name)

    def __str__(self):
        return pformat(self.asdict())


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
    uses_ctd = Column(Boolean, default=False)
    parameters = relationship('Parameter', secondary='stream_parameter')
    binsize_minutes = Column(Integer, nullable=False)
    source_streams = relationship('Stream',
                                  secondary="stream_dependency",
                                  primaryjoin=id == StreamDependency.product_stream_id,
                                  secondaryjoin=id == StreamDependency.source_stream_id)

    product_streams = relationship('Stream',
                                   secondary="stream_dependency",
                                   primaryjoin=id == StreamDependency.source_stream_id,
                                   secondaryjoin=id == StreamDependency.product_stream_id)

    def __repr__(self):
        return 'Stream({:d}: {:s})'.format(self.id, self.name)

    @property
    def needs(self):
        """
        Determine all external parameters needed by this stream to fulfill all derived products
        :return: List of Parameter objects
        """
        return self.needs_external(None)

    @property
    def needs_cc(self):
        function_params = [p for p in self.parameters if p.is_function]
        needed = set([p1 for p in function_params for p1 in p.needs_cc])
        return needed

    @property
    def derived(self):
        return [p for p in self.parameters if p.is_function]

    def needs_external(self, parameters):
        if parameters:
            function_params = [p for p in self.parameters if p in parameters and p.is_function]
        else:
            function_params = [p for p in self.parameters if p.is_function]
        needed = set([p1 for p in function_params for p1 in p.needs])
        local1 = [(None, (p,)) for p in self.parameters]
        local2 = [(self, (p,)) for p in self.parameters]
        return needed.difference(local1).difference(local2)

    def needs_internal(self, parameters):
        """
        Return all RAW parameters from this stream necessary to build the supplied parameter(s)
        :param parameters: list of parameters to check
        :return: set of parameters necessary to complete all derived products
        """
        to_process = {p for p in parameters if p in self.parameters}
        needs = set()
        processed = set()

        while to_process:
            to_consider = to_process.pop()
            if not to_consider.is_function:
                if to_consider in self.parameters:
                    needs.add(to_consider)
                continue

            for stream, poss in to_consider.needs:
                if stream == self:
                    needs.add(poss[0])
                elif stream is None:
                    for param in poss:

                        if param in needs or param in processed:
                            continue
                        to_process.add(param)
                        if param in self.parameters and not param.is_function:
                            needs.add(param)
                        else:
                            processed.add(param)
        return needs

    def create_function_map(self, parameter, supporting_streams=None):
        if parameter not in self.parameters:
            return None

        external = self.needs_external([parameter])

        if external and not supporting_streams:
            return None

        # map parameter name -> (source, value)
        function_map = {}

        for name, value in parameter.needs_map.iteritems():
            source, value = value
            if not isinstance(value, tuple):
                function_map[name] = ('CAL', value)
            elif source is not None and source is supporting_streams:
                function_map[name] = (source, value[0])
            else:
                for param in value:
                    if param in self.parameters and source is None or source == self:
                        function_map[name] = (self, param)
                    else:
                        for external_stream, external_params in external:
                            if external_stream is None:
                                for stream in supporting_streams:
                                    if param in stream.parameters:
                                        function_map[name] = (stream, param)
                            elif external_stream in supporting_streams:
                                function_map[name] = (external_stream, external_params[0])

            if name not in function_map:
                raise UnfulfilledParameterException('Unable to resolve function argument %r %r' % (name, value))

        return function_map
