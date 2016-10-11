'use strict';

/* eslint new-cap: ["error", { "capIsNewExceptionPattern": "^Sequelize\.." }] */
/* eslint no-underscore-dangle: ["error", { "allowAfterThis": true }] */
const Datastore = require('screwdriver-datastore-base');
const schemas = require('screwdriver-data-schema');
const Sequelize = require('sequelize');
const MODELS = schemas.models;
const MODEL_NAMES = Object.keys(MODELS);

/**
 * Converts data from the value stored in the datastore
 * @method decodeFromDialect
 * @param  {String}          dialect Underlying system that we're reading from
 * @param  {SequelizeRow}    content Row that Sequelize returns to us
 * @param  {Object}          model   Screwdriver Data Schema about the Model
 * @return {Promise}                 Decoded Values (object)
 */
function decodeFromDialect(dialect, content, model) {
    if (content === null) {
        return Promise.resolve(null);
    }

    const decodedValues = content.toJSON();

    // Convert non-postgres complex values
    if (dialect !== 'postgres') {
        const fields = model.base.describe().children;

        Object.keys(decodedValues).forEach((fieldName) => {
            const field = fields[fieldName] || {};
            const fieldType = field.type;

            if (fieldType === 'array' || fieldType === 'object') {
                decodedValues[fieldName] = JSON.parse(decodedValues[fieldName]);
            }
        });
    }

    return Promise.resolve(decodedValues);
}

/**
 * Converts data into values safe for the datastore
 * @method encodeToDialect
 * @param  {String}          dialect Underlying system that we're writing to
 * @param  {Object}          content Field=>Value of the things to save
 * @param  {Object}          model   Screwdriver Data Schema about the Model
 * @return {Promise}                 Encoded Values (object)
 */
function encodeToDialect(dialect, content, model) {
    const encodedKeys = Object.keys(content);
    const encodedValues = encodedKeys.map(keyName => content[keyName]);

    return Promise.all(encodedValues).then((promisedValues) => {
        const encodedObject = {};

        // Flatten into an object again
        encodedKeys.forEach((keyName, index) => {
            encodedObject[keyName] = promisedValues[index];
        });

        // Convert non-postgres complex values
        if (dialect !== 'postgres') {
            const fields = model.base.describe().children;

            encodedKeys.forEach((fieldName) => {
                const field = fields[fieldName] || {};
                const fieldType = field.type;

                if (fieldType === 'array' || fieldType === 'object') {
                    encodedObject[fieldName] = JSON.stringify(encodedObject[fieldName]);
                }
            });
        }

        return encodedObject;
    });
}

/**
 * Convert a Joi type into a Sequelize type
 * @method getSequelizeTypeFromJoi
 * @param  {String}          dialect Underlying system that we're writing to
 * @param  {String}          type    Joi type
 * @return {SequelizeType}           Type to use in Sequelize
 */
function getSequelizeTypeFromJoi(dialect, type) {
    switch (type) {
    case 'string':
        return Sequelize.TEXT;
    case 'date':
        return Sequelize.DATE;
    case 'number':
        return Sequelize.DECIMAL;
    case 'boolean':
        return Sequelize.BOOLEAN;
    case 'binary':
        return Sequelize.BLOB;
    case 'array':
        // Unique to postgres, so JSON stringify for others
        return dialect === 'postgres' ? Sequelize.ARRAY(Sequelize.TEXT) : Sequelize.TEXT;
    case 'object':
        // Unique to postgres, so JSON stringify for others
        return dialect === 'postgres' ? Sequelize.JSON : Sequelize.TEXT;
    default:
        return null;
    }
}

class Squeakquel extends Datastore {
    /**
     * Constructs a Squeakquel object
     * http://docs.sequelizejs.com/en/latest/api/sequelize/
     * @param  {Object} [config]                      Configuration object
     * @param  {String} [config.database=screwdriver] Database name
     * @param  {String} [config.dialect=mysql]        Type to use mysql, mssql, postgres, sqlite
     * @param  {String} [config.username]             Login username
     * @param  {String} [config.password]             Login password
     * @param  {String} [config.prefix]               Prefix to add before all table names
     */
    constructor(config = {}) {
        super();

        this.client = new Sequelize(
            config.database || 'screwdriver',
            config.username,
            config.password,
            config
        );
        this.prefix = config.prefix || '';

        this.tables = {};
        this.models = {};

        MODEL_NAMES.forEach((modelName) => {
            const table = this._defineTable(modelName);
            const model = schemas.models[modelName];

            this.tables[model.tableName] = table;
            this.models[model.tableName] = model;
        });
    }

    /**
     * Generate a Sequelize model for a specified model
     * @method _defineTable
     * @param  {String}    modelName Name of the model
     * @param  {String}    [prefix]  Prefix of the table names
     * @return {SequelizeModel}      Sequelize table/model
     */
    _defineTable(modelName) {
        const schema = MODELS[modelName];
        const tableName = `${this.prefix}${schema.tableName}`;
        const fields = schema.base.describe().children;
        const tableFields = {};
        const tableOptions = {
            timestamps: false
        };

        Object.keys(fields).forEach((fieldName) => {
            const field = fields[fieldName];
            const output = {
                type: getSequelizeTypeFromJoi(this.client.getDialect(), field.type)
            };

            if (fieldName === 'id') {
                output.primaryKey = true;
            }

            tableFields[fieldName] = output;
        });

        // @TODO Indexes and Range Keys
        return this.client.define(tableName, tableFields, tableOptions);
    }

    /**
     * Get tables in order
     * @method setup
     * @return {Promise}
     */
    setup() {
        return this.client.sync();
    }

    /**
     * Obtain an item from the table by primary key
     * @param  {Object}   config             Configuration object
     * @param  {String}   config.table       Name of the table to interact with
     * @param  {Object}   config.params      Record Data
     * @param  {String}   config.params.id   ID of the entry to fetch
     * @return {Promise}                     Resolves to the record found from datastore
     */
    _get(config) {
        const table = this.tables[config.table];
        const model = this.models[config.table];

        if (!table) {
            return Promise.reject(new Error(`Invalid table name "${config.table}"`));
        }

        return table.findById(config.params.id)
            .then(item => decodeFromDialect(this.client.getDialect(), item, model));
    }

    /**
     * Save a item in the specified table
     * @param  {Object}   config             Configuration object
     * @param  {String}   config.table       Table name
     * @param  {Object}   config.params      Record data
     * @param  {String}   config.params.id   Unique id. Typically the desired primary key
     * @param  {Object}   config.params.data The data to save
     * @return {Promise}                     Resolves to the record that was saved
     */
    _save(config) {
        const id = config.params.id;
        const userData = config.params.data;
        const table = this.tables[config.table];
        const model = this.models[config.table];

        if (!table) {
            return Promise.reject(new Error(`Invalid table name "${config.table}"`));
        }

        userData.id = id;

        return encodeToDialect(this.client.getDialect(), userData, model)
            .then(item => table.create(item))
            .then(() => userData);
    }

    /**
     * Remove an item from the table by primary key
     * @param  {Object}   config             Configuration object
     * @param  {String}   config.table       Name of the table to interact with
     * @param  {Object}   config.params      Record Data
     * @param  {String}   config.params.id   ID of the entry to remove
     * @return {Promise}                     Resolves to null if remove successfully
     */
    _remove(config) {
        const table = this.tables[config.table];

        if (!table) {
            return Promise.reject(new Error(`Invalid table name "${config.table}"`));
        }

        return table.destroy({
            where: {
                id: config.params.id
            }
        }).then(() => null);
    }

    /**
     * Update a record in the datastore
     * @param  {Object}   config             Configuration object
     * @param  {String}   config.table       Table name
     * @param  {Object}   config.params      Record data
     * @param  {String}   config.params.id   Unique id. Typically the desired primary key
     * @param  {Object}   config.params.data The data to update with
     * @return {Promise}                     Resolves to the record that was updated
     */
    _update(config) {
        const id = config.params.id;
        const userData = config.params.data;
        const table = this.tables[config.table];
        const model = this.models[config.table];

        if (!table) {
            return Promise.reject(new Error(`Invalid table name "${config.table}"`));
        }

        userData.id = id;

        return encodeToDialect(this.client.getDialect(), userData, model)
            .then(item => table.update(item, {
                where: { id }
            }))
            .then(() => userData);
    }

    /**
     * Scan records in the datastore
     * @method scan
     * @param  {Object}   config                Configuration object
     * @param  {String}   config.table          Table name
     * @param  {Object}   [config.params]       index => values to query on
     * @param  {String}   [config.sort]         Sorting option based on GSI range key. Ascending or descending.
     * @return {Promise}                        Resolves to an array of records
     */
    _scan(config) {
        const table = this.tables[config.table];
        const model = this.models[config.table];
        const findParams = {
            where: {}
        };
        let sortKey = 'id';

        if (!table) {
            return Promise.reject(new Error(`Invalid table name "${config.table}"`));
        }

        if (config.params && Object.keys(config.params).length > 0) {
            Object.keys(config.params).forEach((paramName) => {
                const paramValue = config.params[paramName];

                if (Array.isArray(paramValue)) {
                    findParams.where[paramName] = {
                        in: paramValue
                    };
                } else {
                    findParams.where[paramName] = paramValue;
                }

                const indexIndex = (model.indexes && model.rangeKeys)
                    ? model.indexes.indexOf(paramName) : -1;

                if (indexIndex >= 0) {
                    sortKey = model.rangeKeys[indexIndex];
                }
            });
        }

        if (config.sort === 'ascending') {
            findParams.order = [[sortKey, 'ASC']];
        } else {
            findParams.order = [[sortKey, 'DESC']];
        }

        return table.findAll(findParams)
            .then(items => Promise.all(items.map(item =>
                decodeFromDialect(this.client.getDialect(), item, model))));
    }
}

module.exports = Squeakquel;
