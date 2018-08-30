'use strict';

/* eslint-disable no-underscore-dangle */
const assert = require('chai').assert;
const mockery = require('mockery');
const sinon = require('sinon');
const joi = require('joi');

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
                    any: joi.any()
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
            findById: sinon.stub(),
            findOne: sinon.stub(),
            update: sinon.stub()
        };
        sequelizeClientMock = {
            define: sinon.stub().returns(sequelizeTableMock),
            sync: sinon.stub().resolves(),
            getDialect: sinon.stub().returns('sqlite')
        };
        sequelizeRowMock = {
            get: sinon.stub()
        };
        sequelizeMock = sinon.stub().returns(sequelizeClientMock);
        sequelizeMock.STRING = sinon.stub().withArgs(40).returns('VARCHAR(40)');
        sequelizeMock.TEXT = 'TEXT';
        sequelizeMock.DATE = 'DATE';
        sequelizeMock.DOUBLE = 'DOUBLE';
        sequelizeMock.INTEGER = {};
        sequelizeMock.INTEGER.UNSIGNED = 'UNSIGNED INTEGER';
        sequelizeMock.BOOLEAN = 'BOOLEAN';
        sequelizeMock.BLOB = 'BLOB';
        sequelizeMock.JSON = 'JSON';
        sequelizeMock.ARRAY = sinon.stub().returns('ARRAY');
        sequelizeMock.Op = {
            like: 'LIKE',
            in: 'IN'
        };

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
                    type: 'UNSIGNED INTEGER',
                    primaryKey: true,
                    autoIncrement: true
                },
                name: {
                    type: 'TEXT',
                    unique: 'uniquerow'
                }
            });
            assert.calledWith(sequelizeClientMock.define, 'pipelines', {
                id: {
                    type: 'UNSIGNED INTEGER',
                    primaryKey: true,
                    autoIncrement: true
                },
                str: {
                    type: 'TEXT',
                    unique: 'uniquerow'
                },
                date: {
                    type: 'DATE'
                },
                num: {
                    type: 'DOUBLE',
                    unique: 'uniquerow'
                },
                bool: {
                    type: 'BOOLEAN'
                },
                bin: {
                    type: 'BLOB'
                },
                arr: {
                    type: 'TEXT'
                },
                obj: {
                    type: 'TEXT'
                },
                any: {
                    type: null
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
            sequelizeClientMock.sync.resolves('moo');

            return datastore.setup().then((data) => {
                assert.deepEqual(data, 'moo');
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
                bar: null
            };
            const realData = {
                id: 'data',
                key: 'value',
                arr: [1, 2, 3],
                obj: {
                    a: 'b'
                }
            };

            sequelizeTableMock.findById.resolves(responseMock);
            responseMock.toJSON.returns(testData);

            return datastore.get(testParams).then((data) => {
                assert.deepEqual(data, realData);
                assert.calledWith(sequelizeTableMock.findById, testParams.params.id);
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
                bar: null
            };
            const realData = {
                id: 'data',
                key: 'value',
                arr: [1, 2, 3],
                obj: {
                    a: 'b'
                }
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
            sequelizeTableMock.findById.resolves(null);

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

            sequelizeTableMock.findById.rejects(testError);

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
                    scmRepo: {
                        name: 'A'
                    },
                    key: 'value2'
                },
                {
                    id: 'data1',
                    scmRepo: {
                        name: 'B'
                    },
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
            testParams.sortBy = 'scmRepo.name';

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {},
                    order: [['scmRepo.name', 'DESC']]
                });
            });
        });

        it('scans all the data and returns based on search terms', () => {
            const testData = [
                {
                    id: 'data2',
                    scmRepo: '{"name": "Alpha"}',
                    key: 'value2'
                },
                {
                    id: 'data1',
                    scmRepo: '{"name": "Beta"}',
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
            testParams.params = {
                search: {
                    searchField: 'scmRepo',
                    searchTerm: '%name%A%'
                }
            };

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: { scmRepo: { LIKE: '%name%A%' } },
                    order: [['id', 'DESC']]
                });
            });
        });

        it('scans for some data with params', () => {
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
                foo: 'bar',
                baz: [1, 2, 3]
            };

            sequelizeTableMock.findAll.resolves(testInternal);

            return datastore.scan(testParams).then((data) => {
                assert.deepEqual(data, testData);
                assert.calledWith(sequelizeTableMock.findAll, {
                    where: {
                        foo: 'bar',
                        baz: {
                            IN: [1, 2, 3]
                        }
                    },
                    order: [['id', 'DESC']]
                });
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
    });
});
