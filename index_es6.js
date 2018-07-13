'use strict';

const dgram = require('dgram');
const EventEmitter = require('events').EventEmitter;
const util = require('util');

util.inherits(influxLineUdp, EventEmitter);

function influxLineUdp(host, port) {
  const self = this; // eslint-disable-line no-invalid-this
  EventEmitter.call(self);
  self.host = host;
  self.port = port;
  self.socket = dgram.createSocket('udp4');
  return self;
}

module.exports = influxLineUdp;

influxLineUdp.prototype.send = function send(measurement, fields, tags = {}, timestamp) {
  const self = this;
  if (!measurement || typeof measurement !== 'string') {
    return self.emit('error', 'mesurement should be string');
  }

  const _measurement = escape(measurement);

  if (!fields || !isObject(fields)) {
    return self.emit('error', 'fields should be an Object');
  }

  const escaped_fields_array = [];
  const unescaped_fields_keys = Object.keys(fields) || [];
  for (const key of unescaped_fields_keys) {
    const value = fields[key];

    if (key && value !== undefined) {
      const casted = cast(value);

      if (casted != null) {
        escaped_fields_array.push(escape(key) + '=' + casted);
      }
    }
  }
  const escaped_fields_str = escaped_fields_array.join(',');

  let escapedTags = '';

  if (!isObject(tags)) {
    return self.emit('error', 'tags if provied should be an object');
  }

  const escapedTagsArray = [];
  for (const tagKey in tags) {
    if (tagKey && tags[tagKey] && tags[tagKey] !== '') {
      escapedTagsArray.push(escape(tagKey) + '=' + escape(tags[tagKey]));
    }
  }
  escapedTags = escapedTagsArray.join(',');

  const data = `${_measurement}${escapedTags.length > 0 ? ',' + escapedTags : ''} ${escaped_fields_str}${timestamp ? ' ' + timestamp : ''}`; //eslint-disable-line max-len

  if (!self.socket) {
    self.socket = dgram.createSocket('udp4');
  }
  _send(self.socket, data, 0, self.port, self.host);
  return true;
};

function _send(socket, data, offset, port, host) {
  let _data = data;
  if (!Buffer.isBuffer(_data)) {
    _data = new Buffer(_data);
  }
  socket.send(_data, offset, _data.length, port, host);
}

function isString(arg) {
  return typeof arg === 'string' || arg instanceof String;
}

function isBoolean(arg) {
  return typeof arg === 'boolean' || arg instanceof Boolean;
}

function isObject(obj) {
  const type = typeof obj;
  return type === 'function' || type === 'object' && Boolean(obj);
}

function isNumber(arg) {
  return typeof arg === 'number' || arg instanceof Number;
}

function cast(value) {
  if (isString(value)) {
    return '"' + escape(value) + '"';
  }

  if (isBoolean(value)) {
    return value ? 'TRUE' : 'FALSE';
  }

  if (isNumber(value)) {
    // javascript can't tell the difference between 1.0 and 1, so cast all as float
    return parseFloat(value);
  }

  if (isObject(value)) {
    return '"' + escape(value.toString()) + '"';
  }

  return value;
}

function escape(value) {
  let val = value;

  if (isObject(val) || isNumber(val)) {
    val = val.toString();
  }

  if (isBoolean(value)) {
    return value ? 'TRUE' : 'FALSE';
  }

  if (!isString(val)) {
    return null;
  }

  return val ? val.split('').map(function f(character) {
    let _character = character;
    if (_character === ' ' || _character === ',' || _character === '"') {
      _character = '\\' + _character;
    }
    return _character;
  }).join('') : null;
}
