'use strict';

const avsc = require('avsc');

const fetchSchema = require('./fetch-schema');
const pushSchema = require('./push-schema');

const byId = (registry) => (schemaId, msg, parseOptions = null) => (() => new Promise((resolve, reject) => {
  let promise = registry.cache.getById(schemaId);

  if (promise) {
    return resolve(promise);
  }

  promise = fetchSchema(registry, schemaId, parseOptions);

  registry.cache.set(schemaId, promise);

  promise
    .then((result) => registry.cache.set(schemaId, result))
    .catch(reject);

  return resolve(promise);
}))()
  .then((schema) => {
    const encodedMessage = schema.toBuffer(msg);

    const message = Buffer.alloc(encodedMessage.length + 5);
    message.writeUInt8(0);
    message.writeUInt32BE(schemaId, 1);
    encodedMessage.copy(message, 5);
    return message;
  });

const bySchema = (type, registry, pushNewSchemas = true) => (topic, schema, msg, parseOptions = null) => (async () => {
  const schemaString = JSON.stringify(schema);
  const parsedSchema = avsc.parse(schema, parseOptions);
  let id = registry.cache.getBySchema(parsedSchema);
  if (id) {
    return Promise.resolve(id)
  }

  if (pushNewSchemas) {
    return pushSchema(registry, topic, schemaString, type)
      .then(id => registry.cache.set(id, parsedSchema));
  } else {
    const subject = `${topic}-${type}`;
    const versions = await fetchSchema.fetchVersionsBySubject(registry, subject, parseOptions);
    const latestVersion = versions.pop();
    const schemaInfo = await fetchSchema.fetchBySubjectAndVersion(registry, subject, latestVersion, parseOptions);
    if (schemaInfo.schema === schemaString) {
      registry.cache.set(schemaInfo.id, parsedSchema);
      return Promise.resolve(id);
    } else {
      return Promise.reject(new Error('Unable to locate schema in the registry'));
    }
  }
})()
  .then((schemaId) => {
    const encodedMessage = registry.cache.getById(schemaId).toBuffer(msg);

    const message = Buffer.alloc(encodedMessage.length + 5);
    message.writeUInt8(0);
    message.writeUInt32BE(schemaId, 1);
    encodedMessage.copy(message, 5);
    return message;
  });

module.exports = {
  bySchema,
  byId,
}
