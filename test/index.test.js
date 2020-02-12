'use strict';

/* eslint new-cap: ["error", { "capIsNewExceptionPattern": "^Sequelize\.." }] */
/* eslint-disable no-underscore-dangle */
const assert = require('chai').assert;
const mockery = require('mockery');
const sinon = require('sinon');
const joi = require('joi');
const Sequelize = require('sequelize');

sinon.assert.expose(assert, { prefix: '' });

describe('index test', function () {
    const dataSchemaMock = {
        models: {
            pipeline: {
                base: joi.object({
                    id: joi.string().length(40),
                    str: joi.string(),
                    date: joi.date(),
                    num: joi.number(),
                    bool: joi.boolean(),
                    bin: joi.binary(),
                    arr: joi.array(),
                    obj: joi.object(),
                    any: joi.any(),
                    namespace: joi.string(),
                    name: joi.string()
                }),
                tableName: 'pipelines',
                keys: ['num', 'str'],
                indexes: ['str']
            },
            job: {
                base: joi.object({
                    id: joi.string().length(40),
                    name: joi.string()
                }),
                tableName: 'jobs',
                keys: ['name'],
                indexes: ['name'],
                rangeKeys: ['name']
            },
            trigger: {
                base: joi.object({
                    id: joi.string().length(40),
                    src: joi.alternatives().try(
                        joi.object().max(64),
                        joi.string().max(64)),
                    dest: joi.alternatives().try(
                        joi.object().max(64),
                        joi.string().max(64))
                }),
                tableName: 'triggers',
                keys: ['src', 'dest'],
                indexes: ['dest', 'src']
            }
        },
        plugins: {
            datastore: {
                get: joi.object(),
                update: joi.object(),
                remove: joi.object(),
                save: joi.object(),
                scan: joi.object()
            }
        }
    };
    let datastore;
    let Datastore;
    let sequelizeRowMock;
    let sequelizeTableMock;
    let sequelizeQueryGeneratorMock;
    let sequelizeDialectMock;
    let sequelizeClientMock;
    let sequelizeMock;
    let responseMock;

    // Time not important. Only life important.
    this.timeout(5000);

    before(() => {
        sequelizeTableMock = {
            create: sinon.stub(),
            destroy: sinon.stub(),
            findAll: sinon.stub(),
            findByPk: sinon.stub(),
            findOne: sinon.stub(),
            update: sinon.stub()
        };
        sequelizeQueryGeneratorMock = {
            selectQuery: sinon.stub()
        };
        sequelizeDialectMock = {
            QueryGenerator: sequelizeQueryGeneratorMock
        };
        sequelizeClientMock = {
            define: sinon.stub().returns(sequelizeTableMock),
            sync: sinon.stub().resolves(),
            getDialect: sinon.stub().returns('sqlite'),
            literal: sinon.stub(),
            dialect: sequelizeDialectMock
        };
        sequelizeRowMock = {
            get: sinon.stub()
        };
        sequelizeMock = sinon.stub().returns(sequelizeClientMock);
        sequelizeMock.STRING = Sequelize.STRING;
        sequelizeMock.TEXT = Sequelize.TEXT;
        sequelizeMock.DATE = Sequelize.DATE;
        sequelizeMock.DOUBLE = Sequelize.DOUBLE;
        sequelizeMock.INTEGER = Sequelize.INTEGER;
        sequelizeMock.BOOLEAN = Sequelize.BOOLEAN;
        sequelizeMock.BLOB = Sequelize.BLOB;
        sequelizeMock.JSON = Sequelize.JSON;
        sequelizeMock.ARRAY = Sequelize.ARRAY;
        sequelizeMock.Op = {
            in: 'IN',
            like: 'LIKE',
            iLike: 'ILIKE',
            or: 'OR',
            gte: 'GTE',
            lte: 'LTE',
            eq: 'EQ'
        };
        sequelizeMock.col = sinon.stub().returns('col');
        sequelizeMock.fn = sinon.stub().returnsArg(0);

        responseMock = {
            toJSON: sinon.stub()
        };

        mockery.enable({
            useCleanCache: true,
            warnOnUnregistered: false
        });
        mockery.registerMock('sequelize', sequelizeMock);
        mockery.registerMock('screwdriver-data-schema', dataSchemaMock);

        /* eslint-disable global-require */
        Datastore = require('../index');
        /* eslint-enable global-require */
    });

    beforeEach(() => {
        // Reset mocks
        Object.keys(sequelizeTableMock).forEach(key => sequelizeTableMock[key].reset());
        Object.keys(sequelizeRowMock).forEach(key => sequelizeRowMock[key].reset());
        Object.keys(responseMock).forEach(key => responseMock[key].reset());
        sequelizeClientMock.define = sinon.stub().returns(sequelizeTableMock);
        sequelizeClientMock.sync = sinon.stub().resolves();
        sequelizeClientMock.getDialect = sinon.stub().returns('sqlite');

        datastore = new Datastore();
    });

    after(() => {
        mockery.disable();
    });

    describe('constructor', () => {
        it('constructs the clients', () => {
            datastore = new Datastore({
                dialect: 'sqlite'
            });
            assert.calledWith(sequelizeClientMock.define, 'jobs', {
                id: {
                    type: Sequelize.INTEGER.UNSIGNED,
                    primaryKey: true,
                    autoIncrement: true
                },
                name: {
                    type: Sequelize.TEXT,
                    unique: 'uniquerow'
                }
            });
            assert.calledWith(sequelizeClientMock.define, 'triggers', {
                id: {
                    type: Sequelize.INTEGER.UNSIGNED,
                    primaryKey: true,
                    autoIncrement: true
                },
                src: {
                    type: Sequelize.STRING(64),
                    unique: 'uniquerow'
                },
                dest: {
                    type: Sequelize.STRING(64),
                    unique: 'uniquerow'
                }
            });
            assert.calledWith(sequelizeClientMock.define, 'pipelines', {
                id: {
                    type: Sequelize.INTEGER.UNSIGNED,
                    primaryKey: true,
                    autoIncrement: true
                },
                str: {
                    type: Sequelize.TEXT,
                    unique: 'uniquerow'
                },
                date: {
                    type: Sequelize.DATE
                },
                num: {
                    type: Sequelize.DOUBLE,
                    unique: 'uniquerow'
                },
                bool: {
                    type: Sequelize.BOOLEAN
                },
                bin: {
                    type: Sequelize.BLOB
                },
                arr: {
                    type: Sequelize.TEXT
                },
                obj: {
                    type: Sequelize.TEXT('medium')
                },
                any: {
                    type: null
                },
                namespace: {
                    type: Sequelize.TEXT
                },
                name: {
                    type: Sequelize.TEXT
                }
            });
        });

        it('constructs the clients with a prefix', () => {
            datastore = new Datastore({
                dialect: 'sqlite',
                prefix: 'boo_'
            });
            assert.calledWith(sequelizeClientMock.define, 'boo_jobs');
            assert.calledWith(sequelizeClientMock.define, 'boo_pipelines');
            assert.isUndefined(sequelizeMock.lastCall.args[3].prefix);
        });
    });

    describe('sync', () => {
        it('syncs tables', () => {
            const ddlSyncEnabled = 'true';

            sequelizeClientMock.sync.resolves('moo');

            return datastore.setup(ddlSyncEnabled).then((data) => {
                assert.deepEqual(data, 'moo');
            });
        });

        it('doesnt sync tables', () => {
            const ddlSyncEnabled = 'false';

            sequelizeClientMock.sync.resolves(Promise.resolve());

            return datastore.setup(ddlSyncEnabled).then(() => {
                // do nothing
            }).catch(() => {
                assert.fail('this should not get here');
            });
        });
    });

    describe('get', () => {
        it('gets data by id', () => {
            const testParams = {
                table: 'pipelines',
                params: {
                    id: 'someId'
                }
            };
            const testData = {
                id: 'data',
                key: 'value',
                arr: '[1,2,3]',
                obj: '{"a":"b"}',
                bool: '0',
                bar: null
            };
            const realData = {
                id: 'data',
                key: 'value',
                arr: [1, 2, 3],
                obj: {
                    a: 'b'
                },
                bool: false
            };

            sequelizeTableMock.findByPk.resolves(responseMock);
            responseMock.toJSON.returns(testData);

            return datastore.get(testParams).then((data) => {
                assert.deepEqual(data, realData);
                assert.calledWith(sequelizeTableMock.findByPk, testParams.params.id);
            });
        });

        it('gets data without id', () => {
            const testParams = {
                table: 'pipelines',
                params: {
                    field1: 'value1',
                    field2: 'value2'
                }
            };
            const testData = {
                id: 'data',
                key: 'value',
                arr: '[1,2,3]',
                obj: '{"a":"b"}',
                bool: '0',
                bar: null
            };
            const realData = {
                id: 'data',
                key: 'value',
                arr: [1, 2, 3],
                obj: {
                    a: 'b'
                },
                bool: false
            };

            sequelizeTableMock.findOne.resolves(responseMock);
            responseMock.toJSON.returns(testData);

            return datastore.get(testParams).then((data) => {
                assert.deepEqual(data, realData);
                assert.calledWith(sequelizeTableMock.findOne, {
                    where: testParams.params
                });
            });
        });

        it('gracefully understands that no one is returned when it does not exist', () => {
            sequelizeTableMock.findByPk.resolves(null);

            return datastore.get({
                table: 'pipelines',
                params: {
                    id: 'someId'
                }
            }).then(data => assert.isNull(data));
        });

        it('fails when given an unknown table name', () =>
            datastore.get({
                table: 'tableUnicorn',
                params: {
                    id: 'doesNotMatter'
                }
            }).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, /Invalid table name/);
            })
        );

        it('fails when it encounters an error', () => {
            const testError = new Error('errorCommunicatingToApi');

            sequelizeTableMock.findByPk.rejects(testError);

            return datastore._get({
                table: 'pipelines',
                params: {
                    id: 'someId'
                }
            }).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.equal(err.message, testError.message);
            });
        });
    });

    describe('save', () => {
        it('saves the data', () => {
            const expectedResult = {
                id: 'someIdToPutHere',
                key: 'value',
                arr: [1, 2, 3],
                obj: {
                    a: 'b'
                }
            };
            const expectedRow = sequelizeRowMock;

            expectedRow.get.returns(expectedResult);
            sequelizeTableMock.create.resolves(expectedRow);

            return datastore.save({
                table: 'pipelines',
                params: {
                    key: 'value',
                    arr: [1, 2, 3],
                    obj: {
                        a: 'b'
                    }
                }
            }).then((data) => {
                assert.deepEqual(data, expectedResult);
                assert.calledWith(sequelizeTableMock.create, {
                    key: 'value',
                    arr: '[1,2,3]',
                    obj: '{"a":"b"}'
                });
            });
        });

        it('fails when it encounters an error', () => {
            const testError = new Error('testError');

            sequelizeTableMock.create.rejects(testError);

            return datastore.save({
                table: 'pipelines',
                params: {
                    id: 'doesNotMatter',
                    data: {}
                }
            }).then(() => {
                throw new Error('Oops');
            }, (err) => {
                assert.isOk(err, 'Error should be returned');
                assert.equal(err.message, testError.message);
            });
        });

        it('fails when given an unknown table name', () =>
            datastore.save({
                table: 'doesNotExist',
                params: {
                    id: 'doesNotMatter',
                    data: {}
                }
            }).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, /Invalid table name/);
            })
        );
    });

    describe('remove', () => {
        it('removes data by id', () => {
            const testParams = {
                table: 'pipelines',
                params: {
                    id: 'someId'
                }
            };

            sequelizeTableMock.destroy.resolves(null);

            return datastore.remove(testParams).then((data) => {
                assert.isNull(data);
                assert.calledWith(sequelizeTableMock.destroy, {
                    where: {
                        id: testParams.params.id
                    }
                });
            });
        });

        it('fails when given an unknown table name', () =>
            datastore.remove({
                table: 'tableUnicorn',
                params: {
                    id: 'doesNotMatter'
                }
            }).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, /Invalid table name/);
            })
        );

        it('fails when it encounters an error', () => {
            const testError = new Error('errorCommunicatingToApi');

            sequelizeTableMock.destroy.rejects(testError);

            return datastore.remove({
                table: 'pipelines',
                params: {
                    id: 'someId'
                }
            }).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, testError.message);
            });
        });
    });

    describe('update', () => {
        it('updates the data in the datastore', () => {
            const id = 'someId';
            const expectedResult = {
                id,
                targetKey: 'updatedValue'
            };

            sequelizeTableMock.update.resolves();

            return datastore.update({
                table: 'pipelines',
                params: {
                    id,
                    targetKey: 'updatedValue'
                }
            }).then((data) => {
                assert.deepEqual(data, expectedResult);
                assert.calledWith(sequelizeTableMock.update, {
                    id,
                    targetKey: 'updatedValue'
                });
            });
        });

        it('fails when given an unknown table name', () =>
            datastore.update({
                table: 'doesNotExist',
                params: {
                    id: 'doesNotMatter',
                    data: {}
                }
            }).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, /Invalid table name/);
            })
        );

        it('fails when it encounters an error', () => {
            const testError = new Error('testError');

            sequelizeTableMock.update.rejects(testError);

            return datastore.update({
                table: 'pipelines',
                params: {
                    id: 'doesNotMatter',
                    data: {}
                }
            }).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.equal(err.message, testError.message);
            });
        });
    });

    describe('scan', () => {
        let testParams;

        beforeEach(() => {
            testParams = {
                table: 'pipelines'
            };
        });

        it('scans all the data', () => {
            const testData = [
                {
                    id: 'data1',
                    key: 'value1'
                },
                {
                    id: 'data2',
                    key: 'value2'
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns(testData[0])
                },
                {
                    toJSON: sinon.stub().returns(testData[1])
                }
            ];

            sequelizeTableMock.findAll.resolves(testInternal);

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {},
                    order: [['id', 'DESC']]
                });
            });
        });

        it('scans all the data in reverse', () => {
            const testData = [
                {
                    id: 'data1',
                    key: 'value1'
                },
                {
                    id: 'data2',
                    key: 'value2'
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns(testData[0])
                },
                {
                    toJSON: sinon.stub().returns(testData[1])
                }
            ];

            testParams.sort = 'ascending';

            sequelizeTableMock.findAll.resolves(testInternal);

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {},
                    order: [['id', 'ASC']]
                });
            });
        });

        it('scans a page of data', () => {
            const dummyData = [];

            for (let i = 1; i <= 30; i += 1) {
                dummyData.push({
                    id: `data${i}`,
                    key: `value${i}`
                });
            }

            const testData = dummyData.slice(11, 21);
            const testInternal = testData.map(data => ({
                toJSON: sinon.stub().returns(data)
            }));

            testParams.paginate = {
                count: 10,
                page: 2
            };

            sequelizeTableMock.findAll.resolves(testInternal);

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {},
                    order: [['id', 'DESC']],
                    limit: 10,
                    offset: 10
                });
            });
        });

        it('scans all the data and returns sorted by sortBy field', () => {
            const testData = [
                {
                    id: 'data2',
                    str: 'A',
                    key: 'value2'
                },
                {
                    id: 'data1',
                    str: 'B',
                    key: 'value1'
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns(testData[0])
                },
                {
                    toJSON: sinon.stub().returns(testData[1])
                }
            ];

            sequelizeTableMock.findAll.resolves(testInternal);
            testParams.sortBy = 'str';

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {},
                    order: [['str', 'DESC']]
                });
            });
        });

        it('scans all the data and returns based on search values', () => {
            const testData = [
                {
                    id: 'data2',
                    name: 'food',
                    key: 'value2'
                },
                {
                    id: 'data1',
                    name: 'foodie',
                    key: 'value1'
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns(testData[0])
                },
                {
                    toJSON: sinon.stub().returns(testData[1])
                }
            ];

            sequelizeTableMock.findAll.resolves(testInternal);
            testParams.search = {
                field: 'name',
                keyword: '%foo%'
            };

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: { name: { LIKE: '%foo%' } },
                    order: [['id', 'DESC']]
                });
            });
        });

        it('scans all the data and returns based on search values (case insensitive)', () => {
            const testData = [
                {
                    id: 'data2',
                    name: 'food',
                    key: 'value2'
                },
                {
                    id: 'data1',
                    name: 'foodie',
                    key: 'value1'
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns(testData[0])
                },
                {
                    toJSON: sinon.stub().returns(testData[1])
                }
            ];

            sequelizeTableMock.findAll.resolves(testInternal);
            testParams.search = {
                field: 'name',
                keyword: '%foo%'
            };

            sequelizeClientMock.getDialect = sinon.stub().returns('postgres');

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: { name: { ILIKE: '%foo%' } },
                    order: [['id', 'DESC']]
                });
            });
        });

        it('scans all the data and returns based on search values with multiple fields', () => {
            const testData = [
                {
                    id: 'data3',
                    namespace: 'foo',
                    name: 'value3'
                },
                {
                    id: 'data2',
                    namespace: 'screwdriver',
                    name: 'foo'
                },
                {
                    id: 'data1',
                    namespace: 'fool',
                    name: 'value1'
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns(testData[0])
                },
                {
                    toJSON: sinon.stub().returns(testData[1])
                },
                {
                    toJSON: sinon.stub().returns(testData[2])
                }
            ];

            sequelizeTableMock.findAll.resolves(testInternal);
            testParams.search = {
                field: ['namespace', 'name'],
                keyword: '%foo%'
            };

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {
                        OR: [
                            { namespace: { LIKE: '%foo%' } },
                            { name: { LIKE: '%foo%' } }
                        ]
                    },
                    order: [['id', 'DESC']]
                });
            });
        });

        it('throws error if search field does not exist in schema', () => {
            testParams.search = {
                field: 'banana',
                keyword: '%foo%'
            };

            return datastore.scan(testParams).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, /Invalid search field "banana"/);
            });
        });

        it('throws error if search field in field array does not exist in schema', () => {
            testParams.search = {
                field: ['namespace', 'name', 'banana'],
                keyword: '%foo%'
            };

            return datastore.scan(testParams).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, /Invalid search field "banana"/);
            });
        });

        it('throws error if sortBy does not exist in schema', () => {
            testParams.sortBy = 'banana';

            return datastore.scan(testParams).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, /Invalid sortBy "banana"/);
            });
        });

        it('scans for some data with params', () => {
            const testData = [
                {
                    id: 1,
                    name: 'foo'
                },
                {
                    id: 2,
                    name: 'foo'
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns(testData[0])
                },
                {
                    toJSON: sinon.stub().returns(testData[1])
                }
            ];

            testParams.params = {
                name: 'foo',
                id: [1, 2, 3]
            };

            sequelizeTableMock.findAll.resolves(testInternal);

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {
                        name: 'foo',
                        id: {
                            IN: [1, 2, 3]
                        }
                    },
                    order: [['id', 'DESC']]
                });
            });
        });

        it('throws an error when the param is not valid', () => {
            testParams.params = {
                foo: 'banana'
            };

            return datastore.scan(testParams).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, /Invalid param "foo"/);
            });
        });

        it('scans for some data with distinct params', () => {
            const testData = [
                {
                    id: 'data1',
                    namespace: 'tools',
                    key: 'value1'
                },
                {
                    id: 'data1',
                    namespace: 'screwdriver',
                    key: 'value1'
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns(testData[0])
                },
                {
                    toJSON: sinon.stub().returns(testData[1])
                }
            ];

            testParams.params = {
                distinct: 'namespace'
            };

            sequelizeTableMock.findAll.resolves(testInternal);

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {},
                    attributes: [[
                        sequelizeMock.fn('DISTINCT', sequelizeMock.col('namespace')),
                        'namespace'
                    ]],
                    order: [['id', 'DESC']]
                });
            });
        });

        it('throws an error when the distinct field is not valid', () => {
            testParams.params = {
                distinct: 'banana'
            };

            return datastore.scan(testParams).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, /Invalid distinct field "banana"/);
            });
        });

        it('scans for some data with indexed params', () => {
            const testData = [
                {
                    id: 'data1',
                    key: 'value1'
                },
                {
                    id: 'data2',
                    key: 'value2'
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns(testData[0])
                },
                {
                    toJSON: sinon.stub().returns(testData[1])
                }
            ];

            testParams.params = {
                str: 'bar',
                baz: [1, 2, 3]
            };

            sequelizeTableMock.findAll.resolves(testInternal);

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {
                        str: 'bar',
                        baz: {
                            IN: [1, 2, 3]
                        }
                    },
                    order: [['id', 'DESC']]
                });
            });
        });

        it('scans for aggregation of data', () => {
            const testData = [
                {
                    templateId: 7,
                    count: 5
                },
                {
                    templateId: 9,
                    count: 2
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns(testData[0])
                },
                {
                    toJSON: sinon.stub().returns(testData[1])
                }
            ];

            testParams.table = 'jobs';
            testParams.aggregationField = 'templateId';
            testParams.params = {
                templateId: [7, 9, 10]
            };

            sequelizeTableMock.findAll.resolves(testInternal);

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {
                        templateId: {
                            IN: [7, 9, 10]
                        }
                    },
                    order: [['id', 'DESC']],
                    attributes: ['templateId', ['COUNT', 'count']],
                    group: 'templateId'
                });
            });
        });

        it('scans for some data with indexed params using range key', () => {
            const testData = [
                {
                    id: 'data1',
                    key: 'value1'
                },
                {
                    id: 'data2',
                    key: 'value2'
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns(testData[0])
                },
                {
                    toJSON: sinon.stub().returns(testData[1])
                }
            ];

            testParams.table = 'jobs';
            testParams.params = {
                name: 'bar',
                baz: [1, 2, 3]
            };

            sequelizeTableMock.findAll.resolves(testInternal);

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {
                        name: 'bar',
                        baz: {
                            IN: [1, 2, 3]
                        }
                    },
                    order: [['name', 'DESC']]
                });
            });
        });

        it('scans and returns grouped data with excluded field(s)', () => {
            const testData = [
                {
                    id: 'data1',
                    key: 'value1'
                },
                {
                    id: 'data2',
                    key: 'value2'
                },
                {
                    id: 'data3',
                    key: 'value2'
                }
            ];
            const testInternal = [
                {
                    toJSON: sinon.stub().returns({ key: 'value1' })
                },
                {
                    toJSON: sinon.stub().returns({ key: 'value2' })
                }
            ];

            testParams.table = 'jobs';
            testParams.exclude = ['id'];
            testParams.groupBy = ['key'];
            sequelizeQueryGeneratorMock.selectQuery.withArgs('jobs', {
                tableAs: 't',
                attributes: ['MAX'],
                where: { id: { GTE: 'col' }, key: { EQ: 'col' } }
            }).returns('subQuery;');
            sequelizeClientMock.literal.withArgs('(subQuery)').returns('literal');
            sequelizeTableMock.findAll.resolves(testInternal);

            return datastore.scan(testParams).then((data) => {
                assert.notDeepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    attributes: [['col', 'name']],
                    where: { id: { EQ: 'literal' } },
                    order: [['col', 'DESC']]
                });
            });
        });

        it('fails when given an unknown table name', () => {
            sequelizeTableMock.findAll.rejects(new Error('cannot find entries in table'));

            return datastore._scan({
                table: 'tableUnicorn'
            }).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, /Invalid table name/);
            });
        });

        it('fails when it encounters an error', () => {
            const testError = new Error('errorCommunicatingToApi');

            sequelizeTableMock.findAll.rejects(testError);

            return datastore._scan({
                table: 'pipelines'
            }).then(() => {
                throw new Error('Oops');
            }).catch((err) => {
                assert.isOk(err, 'Error should be returned');
                assert.match(err.message, testError.message);
            });
        });

        it('scans for data within date range', () => {
            sequelizeTableMock.findAll.resolves([]);
            testParams.startTime = '2019-01-28T11:00:00.000Z';
            testParams.endTime = '2019-01-28T12:00:00.000Z';

            return datastore.scan(testParams).then(() => {
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {
                        createTime: {
                            GTE: testParams.startTime,
                            LTE: testParams.endTime
                        }
                    },
                    order: [['id', 'DESC']]
                });
            });
        });

        it('scans for data within date range using time key', () => {
            sequelizeTableMock.findAll.resolves([]);
            testParams.startTime = '2019-01-28T11:00:00.000Z';
            testParams.endTime = '2019-01-28T12:00:00.000Z';
            testParams.timeKey = 'startTime';

            return datastore.scan(testParams).then(() => {
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {
                        startTime: {
                            GTE: testParams.startTime,
                            LTE: testParams.endTime
                        }
                    },
                    order: [['id', 'DESC']]
                });
            });
        });
    });
});
