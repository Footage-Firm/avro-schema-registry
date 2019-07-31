'use strict';

const avsc = require('avsc');

const makeRequest = (registry, endpoint, success) => new Promise((resolve, reject) => {
  const {protocol, host, port, auth, path} = registry;
  const requestOptions = {
    host,
    port,
    path: path + endpoint,
    auth
  };
  protocol.get(requestOptions, (res) => {
    let data = '';
    res.on('data', (d) => { data += d });
    res.on('error', (e) => { reject(e) });
    res.on('end', () => {
      if (res.statusCode !== 200) {
        const error = JSON.parse(data);
        return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`));
      }
      resolve(success(data));
    });
  }).on('error', (e) => { reject(e) });
});

const fetch = (registry, schemaId, parseOptions) => new Promise((resolve, reject) => {
  const {protocol, host, port, auth, path} = registry;
  const requestOptions = {
    host,
    port,
    path: `${path}schemas/ids/${schemaId}`,
    auth
  };
  protocol.get(requestOptions, (res) => {
    let data = '';
    res.on('data', (d) => {
      data += d;
    });
    res.on('error', (e) => {
      reject(e);
    });
    res.on('end', () => {
      if (res.statusCode !== 200) {
        const error = JSON.parse(data);
        return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`));
      }

      const schema = JSON.parse(data).schema;
      resolve(avsc.parse(schema, parseOptions));
    });
  }).on('error', (e) => {
    reject(e);
  });
});

const fetchVersionsBySubject = (registry, subject) =>
  makeRequest(registry, `subjects/${subject}/versions`, (data) => JSON.parse(data) );

const fetchBySubjectAndVersion = (registry, subject, version) =>
  makeRequest(registry, `subjects/${subject}/versions/${version}`, (data) => JSON.parse(data) );


module.exports = {
  fetch,
  fetchVersionsBySubject,
  fetchBySubjectAndVersion
};

