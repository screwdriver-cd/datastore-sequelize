'use strict';

/* eslint new-cap: ["error", { "capIsNewExceptionPattern": "^Sequelize\.." }] */
/* eslint no-underscore-dangle: ["error", { "allowAfterThis": true }] */

const Datastore = require('screwdriver-datastore-base');
const schemas = require('screwdriver-data-schema');
const Sequelize = require('sequelize');
const MODELS = schemas.models;
const MODEL_NAMES = Object.keys(MODELS);
const logger = require('screwdriver-logger');

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
    const { fields } = model;

    Object.keys(decodedValues).forEach(fieldName => {
        const field = fields[fieldName] || {};
        const fieldType = field.type;

        if (fieldType === 'array' || fieldType === 'object') {
            decodedValues[fieldName] = JSON.parse(decodedValues[fieldName]);
        }

        if (fieldType === 'boolean') {
            if (typeof decodedValues[fieldName] === 'string') {
                if (decodedValues[fieldName] === '1') {
                    decodedValues[fieldName] = true;
                } else if (decodedValues[fieldName] === '0') {
                    decodedValues[fieldName] = false;
                }
            }
        }

        if (decodedValues[fieldName] === null) {
            delete decodedValues[fieldName];
        }
    });

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

    return Promise.all(encodedValues).then(promisedValues => {
        const encodedObject = {};

        // Flatten into an object again
        encodedKeys.forEach((keyName, index) => {
            encodedObject[keyName] = promisedValues[index];
        });

        const { fields } = model;

        encodedKeys.forEach(fieldName => {
            const field = fields[fieldName] || {};
            const fieldType = field.type;

            if (fieldType === 'array' || fieldType === 'object') {
                encodedObject[fieldName] = JSON.stringify(encodedObject[fieldName]);
            }
        });

        return encodedObject;
    });
}

/**
 * Convert a Joi type into a Sequelize type
 * @method getSequelizeTypeFromJoi
 * @param  {String}          dialect Underlying system that we're writing to
 * @param  {String}          type    Joi type
 * @param  {Array}           rules   Joi rules array
 * @return {SequelizeType}           Type to use in Sequelize
 */
function getSequelizeTypeFromJoi(dialect, type, rules) {
    // Get the column length if length/max rule is included in dataschema
    let length;
    const column = rules.filter(o => o.name === 'length' || o.name === 'max');

    if (column) {
        const args = column.map(o => o.arg)[0] || column.map(o => o.args)[0];

        if (args) {
            length = args.limit;
        }
    }

    switch (type) {
        case 'string':
            if (length) {
                return Sequelize.STRING(length);
            }

            return Sequelize.TEXT;
        case 'array':
            return Sequelize.TEXT;
        case 'object':
            return Sequelize.TEXT('medium');
        case 'date':
            return Sequelize.DATE;
        case 'number':
            return Sequelize.DOUBLE;
        case 'boolean':
            return Sequelize.BOOLEAN;
        case 'binary':
            return Sequelize.BLOB;
        case 'alternatives':
            if (length) {
                return Sequelize.STRING(length);
            }

            return Sequelize.TEXT('medium');
        default:
            return null;
    }
}

class Squeakquel extends Datastore {
    /**
     * Constructs a Squeakquel object
     * http://docs.sequelizejs.com/en/latest/api/sequelize/
     * @param  {Object}  [config]                       Configuration object
     * @param  {String}  [config.database=screwdriver]  Database name
     * @param  {String}  [config.dialect=mysql]         Type to use mysql, mssql, postgres, sqlite
     * @param  {String}  [config.username]              Login username
     * @param  {String}  [config.password]              Login password
     * @param  {String}  [config.prefix]                Prefix to add before all table names
     * @param  {Integer} [config.slowlogThreshold=1000] Threshold for logging slowlogs in ms
     */
    constructor(config = {}) {
        super();

        this.slowlogThreshold = config.slowlogThreshold || 1000;

        config.benchmark = true;
        config.logging = (log, time) => {
            if (time >= this.slowlogThreshold) {
                logger.info(`Slow log detected: ${log}, executed in ${time}ms`);
            }
        };
        this.prefix = config.prefix || '';

        // It won't work if prefix is passed to Sequelize
        delete config.prefix;

        this.client = new Sequelize(config.database || 'screwdriver', config.username, config.password, config);

        this.tables = {};
        this.models = {};

        MODEL_NAMES.forEach(modelName => {
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
        const { fields } = schema;
        const tableFields = {};
        const tableOptions = {
            timestamps: false,
            indexes: schema.indexes
        };

        Object.keys(fields).forEach(fieldName => {
            const field = fields[fieldName].describe();

            let rules;

            if (field.alternatives) {
                // For schema using alternatives like triggers table.
                rules = field.alternatives[0].rules;
            } else if (field.matches && field.matches[0] && field.matches[0].schema) {
                rules = field.matches[0].schema.rules;
            } else {
                rules = field.rules;
            }

            const output = {
                type: getSequelizeTypeFromJoi(this.client.getDialect(), field.type, rules || [])
            };

            if (fieldName === 'id') {
                output.primaryKey = true;
                output.autoIncrement = true;
                output.type = Sequelize.INTEGER.UNSIGNED;
            }

            if (schema.keys.indexOf(fieldName) !== -1) {
                output.unique = 'uniquerow';
            }

            tableFields[fieldName] = output;
        });

        // @TODO Indexes and Range Keys
        return this.client.define(tableName, tableFields, tableOptions);
    }

    /**
     * Database schema will be synced based on syncViaAPI environment variable
     * @method setup
     * @return {Promise}
     */
    setup(ddlSyncEnabled) {
        logger.info(`Datastore ddl sync enabled: ${ddlSyncEnabled}`);
        if (ddlSyncEnabled === 'true') {
            return this.client.sync({ alter: true });
        }

        return Promise.resolve();
    }

    /**
     * Obtain an item from the table by primary key
     * @param  {Object}   config             Configuration object
     * @param  {String}   config.table       Name of the table to interact with
     * @param  {Object}   config.params      Record Data
     * @param  {String}   [config.params.id] ID of the entry to fetch
     * @return {Promise}                     Resolves to the record found from datastore
     */
    _get(config) {
        const table = this.tables[config.table];
        const model = this.models[config.table];
        let finder;

        if (!table) {
            return Promise.reject(new Error(`Invalid table name "${config.table}"`));
        }

        if (config.params.id === undefined) {
            finder = table.findOne({
                where: config.params
            });
        } else {
            finder = table.findByPk(config.params.id);
        }

        return finder.then(item => decodeFromDialect(this.client.getDialect(), item, model));
    }

    /**
     * Save a item in the specified table
     * @param  {Object}   config             Configuration object
     * @param  {String}   config.table       Table name
     * @param  {Object}   config.params      Record data
     * @return {Promise}                     Resolves to the record that was saved
     */
    _save(config) {
        const userData = config.params;
        const table = this.tables[config.table];
        const model = this.models[config.table];

        if (!table) {
            return Promise.reject(new Error(`Invalid table name "${config.table}"`));
        }

        return encodeToDialect(this.client.getDialect(), userData, model)
            .then(item => table.create(item))
            .then(row =>
                row.get({
                    plain: true
                })
            );
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

        return table
            .destroy({
                where: {
                    id: config.params.id
                }
            })
            .then(() => null);
    }

    /**
     * Update a record in the datastore
     * @param  {Object}   config             Configuration object
     * @param  {String}   config.table       Table name
     * @param  {Object}   config.params      Record data
     * @param  {String}   config.params.id   Unique id. Typically the desired primary key
     * @return {Promise}                     Resolves to the record that was updated
     */
    _update(config) {
        const { id } = config.params;
        const userData = config.params;
        const table = this.tables[config.table];
        const model = this.models[config.table];

        if (!table) {
            return Promise.reject(new Error(`Invalid table name "${config.table}"`));
        }

        return encodeToDialect(this.client.getDialect(), userData, model)
            .then(item =>
                table.update(item, {
                    where: { id }
                })
            )
            .then(() => userData);
    }

    /**
     * Returns whether the field is a valid field in the table model or not
     * @param  {Array}      validFields List of valid fields
     * @param  {String}     field       Field to check
     * @return {Boolean}                Returns true if field is invalid
     */
    _fieldInvalid({ validFields, field }) {
        return !validFields.includes(field);
    }

    /**
     * Scan records in the datastore
     * @method scan
     * @param  {Object}         config                    Configuration object
     * @param  {Array<String>}  [config.exclude]          Attribute(s) to discard
     * @param  {Array<String>}  [config.groupBy]          Attribute(s) to group rows by
     * @param  {String}         [config.table]            Table name
     * @param  {Object}         [config.paginate]         Pagination parameters
     * @param  {Number}         [config.paginate.count]   Number of items per page
     * @param  {Number}         [config.paginate.page]    Specific page of the set to return
     * @param  {Boolean}        [config.getCount]         Get total count of record matching query criteria
     * @param  {Object}         [config.params]           index => values to query on
     * @param  {String}         [config.params.distinct]  Field to return distinct rows on
     * @param  {Object}         [config.search]           Search parameters
     * @param  {String|Array}   [config.search.field]     Search field (eg: jobName)
     * @param  {String|Array}   [config.search.keyword]   Search keyword (eg: main)
     * @param  {String}         [config.sort]             Sorting option based on GSI range key. Ascending or descending.
     * @param  {String}         [config.sortBy]           Key to sort by; defaults to 'id'
     * @param  {String}         [config.startTime]        Search for records >= startTime
     * @param  {String}         [config.endTime]          Search for records <= endTime
     * @param  {String}         [config.aggregationField] Field that will be aggregated in aggregation query
     * @return {Promise}                                  Resolves to an array of records or an object
     */
    _scan(config) {
        const table = this.tables[config.table];
        const model = this.models[config.table];
        const tableName = `${this.prefix}${config.table}`;
        const findParams = {
            where: {}
        };
        let sortKey = 'id';

        if (!table) {
            return Promise.reject(new Error(`Invalid table name "${config.table}"`));
        }

        const { fields } = model;
        const validFields = Object.keys(fields);

        if (config.paginate) {
            findParams.limit = config.paginate.count;
            findParams.offset = findParams.limit * (config.paginate.page - 1);
        }

        if (config.params && Object.keys(config.params).length > 0) {
            Object.keys(config.params).forEach(paramName => {
                const paramValue = config.params[paramName];

                if (Array.isArray(paramValue)) {
                    findParams.where[paramName] = {
                        [Sequelize.Op.in]: paramValue
                    };
                    // Return distinct rows
                } else if (paramName === 'distinct') {
                    if (this._fieldInvalid({ validFields, field: paramValue })) {
                        throw new Error(`Invalid distinct field "${paramValue}"`);
                    }
                    findParams.attributes = [[Sequelize.fn('DISTINCT', Sequelize.col(paramValue)), paramValue]];
                } else {
                    if (this._fieldInvalid({ validFields, field: paramName })) {
                        throw new Error(`Invalid param "${paramName}"`);
                    }
                    findParams.where[paramName] = paramValue;
                }

                const indexIndex = model.indexes && model.rangeKeys ? model.indexes.indexOf(paramName) : -1;

                if (indexIndex >= 0) {
                    sortKey = model.rangeKeys[indexIndex];
                }
            });
        }

        if (config.search && config.search.field && config.search.keyword) {
            let searchOperator = this.client.getDialect() === 'postgres' ? Sequelize.Op.iLike : Sequelize.Op.like;

            // If field or keyword is array, search for all keywords in all fields
            if (Array.isArray(config.search.field) || Array.isArray(config.search.keyword)) {
                const searchFields = Array.isArray(config.search.field) ? config.search.field : [config.search.field];
                const searchKeywords = Array.isArray(config.search.keyword)
                    ? config.search.keyword
                    : [config.search.keyword];

                findParams.where = {
                    ...findParams.where,
                    [Sequelize.Op.or]: []
                };

                searchOperator = Number.isInteger(searchKeywords[0]) ? Sequelize.Op.eq : searchOperator;

                searchFields.forEach(field => {
                    if (this._fieldInvalid({ validFields, field })) {
                        throw new Error(`Invalid search field "${field}"`);
                    }
                    searchKeywords.forEach(keyword =>
                        findParams.where[Sequelize.Op.or].push({
                            [field]: {
                                [searchOperator]: keyword
                            }
                        })
                    );
                });
                // If field is string, search using field directly
            } else {
                if (this._fieldInvalid({ validFields, field: config.search.field })) {
                    throw new Error(`Invalid search field "${config.search.field}"`);
                }
                findParams.where[config.search.field] = {
                    [searchOperator]: config.search.keyword
                };
            }
        }

        // if query has startTime and endTime (for metrics)
        const timeKey = config.timeKey || 'createTime';

        if (config.startTime) {
            findParams.where[timeKey] = { [Sequelize.Op.gte]: config.startTime };
        }

        if (config.endTime) {
            findParams.where[timeKey] = findParams.where[timeKey] || {}; // in case there is no startTime
            Object.assign(findParams.where[timeKey], { [Sequelize.Op.lte]: config.endTime });
        }

        if (config.sortBy) {
            if (this._fieldInvalid({ validFields, field: config.sortBy })) {
                return Promise.reject(new Error(`Invalid sortBy "${config.sortBy}"`));
            }
            sortKey = config.sortBy;
        }

        // if query is simply excluding fields
        if (!config.groupBy && Array.isArray(config.exclude)) {
            if (!findParams.attributes) {
                findParams.attributes = {};
            }

            findParams.attributes.exclude = [...config.exclude];
        }

        if (Array.isArray(config.groupBy)) {
            let includedFields = validFields;

            // exclude fields in a group by query
            if (Array.isArray(config.exclude)) {
                includedFields = includedFields.filter(f => !config.exclude.includes(f));
            }

            // every other selected field must be aggregated so database engine won't complain
            // use "MAX" since the nature of this table is append-only
            findParams.attributes = includedFields.map(field => {
                let col = Sequelize.col(field);

                // Temporary treatment to show correct trusted value.
                // This subQuery is used on fiels of SELECT clause.
                // This needs to delete after the trusted table generated.
                if (field === 'trusted') {
                    let subCol = Sequelize.col('trusted');

                    subCol = Sequelize.cast(subCol, 'integer');

                    const subQueryForTrusted = this.client.dialect.QueryGenerator.selectQuery(tableName, {
                        tableAs: 't1',
                        attributes: [Sequelize.fn('MAX', subCol)],
                        where: {
                            name: {
                                [Sequelize.Op.eq]: Sequelize.col(`${tableName}.name`)
                            },
                            namespace: {
                                [Sequelize.Op.eq]: Sequelize.col(`${tableName}.namespace`)
                            }
                        }
                    }).slice(0, -1);

                    col = this.client.literal(`(${subQueryForTrusted})`);
                }
                // Cast boolean to integer
                // It is safer for most dialect to cast to integer instead of other integer type like smallint
                if (fields[field] && fields[field].type === 'boolean') {
                    col = Sequelize.cast(col, 'integer');
                }

                return [col, field];
            });

            sortKey = Sequelize.col(sortKey);

            const where = { id: { [Sequelize.Op.gte]: Sequelize.col(`${tableName}.id`) } };

            config.groupBy.forEach(v => {
                where[v] = { [Sequelize.Op.eq]: Sequelize.col(`${tableName}.${v}`) };
            });

            // slice() method deletes `;`
            const subQuery = this.client.dialect.QueryGenerator.selectQuery(tableName, {
                tableAs: 't',
                attributes: [Sequelize.fn('MAX', Sequelize.col('t.id'))],
                where
            }).slice(0, -1);

            findParams.where = { id: { [Sequelize.Op.eq]: this.client.literal(`(${subQuery})`) } };
        }

        if (config.sort === 'ascending') {
            findParams.order = [[sortKey, 'ASC']];
        } else {
            findParams.order = [[sortKey, 'DESC']];
        }

        if (config.aggregationField) {
            if (!findParams.attributes) {
                findParams.attributes = [];
            }

            findParams.attributes.push(config.aggregationField);
            findParams.attributes.push([Sequelize.fn('COUNT', Sequelize.col(config.aggregationField)), 'count']);

            findParams.group = config.aggregationField;
            delete findParams.order;
        }

        if (config.getCount) {
            return table.findAndCountAll(findParams).then(result =>
                Promise.all(result.rows.map(item => decodeFromDialect(this.client.getDialect(), item, model))).then(
                    rows => {
                        result.rows = rows;

                        return Promise.resolve(result);
                    }
                )
            );
        }

        return table
            .findAll(findParams)
            .then(items => Promise.all(items.map(item => decodeFromDialect(this.client.getDialect(), item, model))));
    }

    /**
     * Run raw query on the datastore
     * @param  {Object}        config                Configuration object
     * @param  {Array<Object>} [config.queries]      Map of database type to query
     * @param  {String}        [config.table]        Table name
     * @param  {Object}        [config.replacements] Parameters to replace in the query
     * @param  {Boolean}       [config.rawResponse]  Return raw response without binding to model
     */
    _query(config) {
        const dialect = this.client.getDialect();
        const table = this.tables[config.table];
        const model = this.models[config.table];
        const query = config.queries.find(q => q.dbType === dialect);
        const queryParams = { replacements: config.replacements };

        if (!table) {
            return Promise.reject(new Error(`Invalid table name "${config.table}"`));
        }
        if (!query) {
            return Promise.reject(new Error(`No query found for "${dialect}" database`));
        }

        if (!config.rawResponse) {
            queryParams.model = this.client.models[config.table];
            queryParams.mapToModel = true;
        }

        return table.sequelize.query(query.query, queryParams).then(data => {
            if (!config.rawResponse) {
                data.map(d => decodeFromDialect(this.client.getDialect(), d, model));
            }

            return data;
        });
    }
}

module.exports = Squeakquel;
