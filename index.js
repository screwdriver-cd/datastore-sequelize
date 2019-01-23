'use strict';

/* eslint new-cap: ["error", { "capIsNewExceptionPattern": "^Sequelize\.." }] */
/* eslint no-underscore-dangle: ["error", { "allow": ["_previousDataValues", "_defineTable", "_fieldInvalid"] }] */

const Datastore = require('screwdriver-datastore-base');
const schemas = require('screwdriver-data-schema');
const Sequelize = require('sequelize');
const winston = require('winston');
const MODELS = schemas.models;
const MODEL_NAMES = Object.keys(MODELS);

const logger = winston.createLogger({
    transports: [
        new winston.transports.Console()
    ]
});

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
    const fields = model.base.describe().children;

    Object.keys(decodedValues).forEach((fieldName) => {
        const field = fields[fieldName] || {};
        const fieldType = field.type;

        if (fieldType === 'array' || fieldType === 'object') {
            decodedValues[fieldName] = JSON.parse(decodedValues[fieldName]);
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

    return Promise.all(encodedValues).then((promisedValues) => {
        const encodedObject = {};

        // Flatten into an object again
        encodedKeys.forEach((keyName, index) => {
            encodedObject[keyName] = promisedValues[index];
        });

        const fields = model.base.describe().children;

        encodedKeys.forEach((fieldName) => {
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
    const length = rules.filter(o => o.name === 'length' || o.name === 'max').map(o => o.arg)[0];

    switch (type) {
    case 'string':
        if (length) {
            return Sequelize.STRING(length);
        }

        return Sequelize.TEXT;
    case 'array':
    case 'object':
        return Sequelize.TEXT;
    case 'date':
        return Sequelize.DATE;
    case 'number':
        return Sequelize.DOUBLE;
    case 'boolean':
        return Sequelize.BOOLEAN;
    case 'binary':
        return Sequelize.BLOB;
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

        config.logging = () => {};
        this.prefix = config.prefix || '';
        this.captureBuildMetrics = config.captureBuildMetrics || 'false';

        // It won't work if prefix is passed to Sequelize
        delete config.prefix;

        this.client = new Sequelize(
            config.database || 'screwdriver',
            config.username,
            config.password,
            config
        );

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
            timestamps: false,
            indexes: schema.indexes
        };

        Object.keys(fields).forEach((fieldName) => {
            const field = fields[fieldName];
            const output = {
                type: getSequelizeTypeFromJoi(
                    this.client.getDialect(),
                    field.type,
                    field.rules || []
                )
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
        const tableObj = this.client.define(tableName, tableFields, tableOptions);

        try {
            if (modelName === 'build' && this.captureBuildMetrics.toString() === 'true') {
                tableObj.afterCreate('afterCreateHook', (instance, options) => {
                    this.upsertBuildReportTable(instance, options, this.prefix);
                });
                tableObj.afterUpdate('afterUpdateHook', (instance, options) => {
                    this.upsertBuildReportTable(instance, options, this.prefix);
                });
            }
        } catch (err) {
            logger.error(`Error upserting build metrics table: ${err}`);
        }

        return tableObj;
    }

    /**
     * upsertBuildReportTable - any change in build status, upsert into flattened build metrics table
     * @param  {Object} instance build table object
     * @param  {Object} options  timestamps and indexes
     * @param  {String} prefix   schema prefix - beta or blank
     */
    upsertBuildReportTable(instance, options, prefix) {
        logger.info(`Started capturing build metrics for build id: ${instance.id}`);
        if (instance._previousDataValues.status !== instance.dataValues.status) {
            this.client.query(`INSERT INTO "${prefix}buildReports" ( ` +
              ' "buildId", "jobId", "parentBuildId", "number", "created", ' +
              ' "createdWeek", "createTime", "startTime", "endTime", ' +
              ' "status", "statusMessage", "cluster", "stats", "totalSteps", ' +
              ' "stepsDuration", "steps", "eventId", "eventCreateTime", ' +
              ' "eventType", "startFrom", "parentEventId", "pr", "commit", ' +
              ' "causeMessage", "pipelineId", "scmUri", "scmContext", ' +
              ' "pipelineCreateTime", "pipelineLastEventId")' +
            ' SELECT b.id::bigint as "buildId", b."jobId"::bigint as "jobId", ' +
                ' b."parentBuildId" as parentBuildId, "number" as number,' +
                ' b."createTime"::date as "created", ' +
                ' date_trunc(\'week\', b."createTime"::date)::date as "createdWeek",' +
                ' b."createTime"::timestamp as "createTime", ' +
                ' b."createTime"::timestamp as "createTime", ' +
                ' b."endTime"::timestamp as "endTime", b.status as "status", ' +
                ' b."statusMessage", b."buildClusterName" as "cluster", ' +
                ' b.stats as "stats", s."totalSteps", s."stepsDuration", s."steps", ' +
                ' e."eventId", e."eventCreateTime", e."eventType", e."startFrom", ' +
                ' e."parentEventId", e.pr, e.commit, e."causeMessage", p."pipelineId", ' +
                ' p."scmUri", p."scmContext", p."pipelineCreateTime", p."pipelineLastEventId"' +
            ` FROM "${prefix}builds" b ` +
              ' INNER JOIN (SELECT id::bigint as "eventId", ' +
                ' "createTime"::timestamp as "eventCreateTime", ' +
                ' "pipelineId" as e_pipelineid, "type" as "eventType", "startFrom", ' +
                ' "parentEventId"::bigint as "parentEventId", "pr", "commit", ' +
                ' "causeMessage" ' +
                     ` FROM "${prefix}events" WHERE id=:eventId) e ` +
                     ' ON e."eventId"=b."eventId" ' +
              ' INNER JOIN (SELECT id::bigint as "pipelineId", "scmUri", "scmContext", ' +
                ' "createTime"::timestamp as "pipelineCreateTime", ' +
                  ' "lastEventId"::bigint as "pipelineLastEventId" ' +
                  ` FROM "${prefix}pipelines") p ` +
                      ' ON p."pipelineId"=e.e_pipelineid ' +
              ' LEFT JOIN (SELECT "buildId" as "buildId", count(id) as "totalSteps", ' +
                ' max("endTime"::timestamp)-min("startTime"::timestamp) as "stepsDuration", ' +
                ' json_agg(json_build_object(\'id\',id,\'name\',name,\'starttime\',"startTime",' +
                ' \'endtime\',"endTime",\'duration\', ' +
                ' "endTime"::timestamp - "startTime"::timestamp ) ) as "steps" ' +
                  ` FROM "${prefix}steps" ` +
                    ' WHERE "buildId"=:buildId' +
                    ' GROUP BY "buildId") s ON s."buildId"=b.id ' +
            ' WHERE b.id=:buildId ' +
                ' ON CONFLICT ("buildId") DO UPDATE SET ' +
                ' "buildId"=excluded."buildId", ' +
                ' "jobId"=excluded."jobId", ' +
                ' "parentBuildId"=excluded."parentBuildId", ' +
                ' "number"=excluded."number", ' +
                ' "created"=excluded."created", ' +
                ' "createdWeek"=excluded."createdWeek", ' +
                ' "createTime"=excluded."createTime", ' +
                ' "startTime"=excluded."startTime", ' +
                ' "endTime"=excluded."endTime", ' +
                ' "status"=excluded."status", ' +
                ' "cluster"=excluded."cluster", ' +
                ' "stats"=excluded."stats", ' +
                ' "totalSteps"=excluded."totalSteps", ' +
                ' "stepsDuration"=excluded."stepsDuration", ' +
                ' "steps"=excluded."steps", ' +
                ' "eventId"=excluded."eventId", ' +
                ' "eventCreateTime"=excluded."eventCreateTime", ' +
                ' "eventType"=excluded."eventType", ' +
                ' "startFrom"=excluded."startFrom", ' +
                ' "parentEventId"=excluded."parentEventId", ' +
                ' "pr"=excluded."pr", ' +
                ' "commit"=excluded."commit", ' +
                ' "causeMessage"=excluded."causeMessage", ' +
                ' "pipelineId"=excluded."pipelineId", ' +
                ' "scmUri"=excluded."scmUri", ' +
                ' "scmContext"=excluded."scmContext", ' +
                ' "pipelineCreateTime"=excluded."pipelineCreateTime", ' +
                ' "pipelineLastEventId"=excluded."pipelineLastEventId"; ',
            { replacements: { buildId: instance.id, eventId: instance.eventId },
                type: this.client.QueryTypes.SELECT
            }
            ).spread((results, metadata) => {
                logger.info(`Upserted ${metadata} records into build metrics table ` +
                   `for buildId: ${instance.id}`);
            });
        }
        logger.info(`Finished capturing build metrics for build id: ${instance.id}`);
    }

    /**
     * Get tables in order
     * @method setup
     * @return {Promise}
     */
    setup() {
        return this.client.sync({ alter: true });
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
            finder = table.findById(config.params.id);
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
            .then(row => row.get({
                plain: true
            }));
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
     * @return {Promise}                     Resolves to the record that was updated
     */
    _update(config) {
        const id = config.params.id;
        const userData = config.params;
        const table = this.tables[config.table];
        const model = this.models[config.table];

        if (!table) {
            return Promise.reject(new Error(`Invalid table name "${config.table}"`));
        }

        return encodeToDialect(this.client.getDialect(), userData, model)
            .then(item => table.update(item, {
                where: { id }
            }))
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
     * @param  {Object}         [config.params]           index => values to query on
     * @param  {String}         [config.params.distinct]  Field to return distinct rows on
     * @param  {Object}         [config.search]           Search parameters
     * @param  {String|Array}   [config.search.field]     Search field (eg: jobName)
     * @param  {String}         [config.search.keyword]   Search keyword (eg: main)
     * @param  {String}         [config.sort]             Sorting option based on GSI range key. Ascending or descending.
     * @param  {String}         [config.sortBy]           Key to sort by; defaults to 'id'
     * @return {Promise}                                  Resolves to an array of records
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

        const validFields = Object.keys(model.base.describe().children);

        if (config.paginate) {
            findParams.limit = config.paginate.count;
            findParams.offset = findParams.limit * (config.paginate.page - 1);
        }

        if (config.params && Object.keys(config.params).length > 0) {
            Object.keys(config.params).forEach((paramName) => {
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
                    findParams.attributes = [[
                        Sequelize.fn('DISTINCT', Sequelize.col(paramValue)), paramValue
                    ]];
                } else {
                    if (this._fieldInvalid({ validFields, field: paramName })) {
                        throw new Error(`Invalid param "${paramName}"`);
                    }
                    findParams.where[paramName] = paramValue;
                }

                const indexIndex = (model.indexes && model.rangeKeys)
                    ? model.indexes.indexOf(paramName) : -1;

                if (indexIndex >= 0) {
                    sortKey = model.rangeKeys[indexIndex];
                }
            });
        }

        if (config.search && config.search.field && config.search.keyword) {
            // If field is array, search for keyword in all fields
            if (Array.isArray(config.search.field)) {
                findParams.where = {
                    [Sequelize.Op.or]: []
                };

                config.search.field.forEach((field) => {
                    if (this._fieldInvalid({ validFields, field })) {
                        throw new Error(`Invalid search field "${field}"`);
                    }
                    findParams.where[Sequelize.Op.or].push({
                        [field]: { [Sequelize.Op.like]: config.search.keyword }
                    });
                });
            // If field is string, search using field directly
            } else {
                if (this._fieldInvalid({ validFields, field: config.search.field })) {
                    throw new Error(`Invalid search field "${config.search.field}"`);
                }
                findParams.where[config.search.field] = {
                    [Sequelize.Op.like]: config.search.keyword
                };
            }
        }

        if (config.sortBy) {
            if (this._fieldInvalid({ validFields, field: config.sortBy })) {
                return Promise.reject(new Error(`Invalid sortBy "${config.sortBy}"`));
            }
            sortKey = config.sortBy;
        }

        // if query is simply excluding fields
        if (!config.groupBy && Array.isArray(config.exclude)) {
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
            findParams.attributes = includedFields.map(field =>
                [Sequelize.fn('MAX', Sequelize.col(field)), field]
            );
            findParams.group = [...config.groupBy];
            sortKey = Sequelize.fn('MAX', Sequelize.col(sortKey));
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
