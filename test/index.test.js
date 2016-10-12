'use strict';

/* eslint-disable no-underscore-dangle */
const assert = require('chai').assert;
const mockery = require('mockery');
const sinon = require('sinon');
const joi = require('joi');

sinon.assert.expose(assert, { prefix: '' });

require('sinon-as-promised');

describe('index test', () => {
    let datastore;
    let Datastore;
    let dataSchemaMock;
    let sequelizeTableMock;
    let sequelizeClientMock;
    let sequelizeMock;
    let responseMock;

    before(() => {
        mockery.enable({
            useCleanCache: true,
            warnOnUnregistered: false
        });
    });

    beforeEach(() => {
        sequelizeTableMock = {
            create: sinon.stub(),
            destroy: sinon.stub(),
            findAll: sinon.stub(),
            findById: sinon.stub(),
            update: sinon.stub()
        };
        sequelizeClientMock = {
            define: sinon.stub().returns(sequelizeTableMock),
            sync: sinon.stub().resolves(),
            getDialect: sinon.stub().returns('sqlite')
        };
        sequelizeMock = sinon.stub().returns(sequelizeClientMock);
        sequelizeMock.TEXT = 'TEXT';
        sequelizeMock.DATE = 'DATE';
        sequelizeMock.DECIMAL = 'DECIMAL';
        sequelizeMock.BOOLEAN = 'BOOLEAN';
        sequelizeMock.BLOB = 'BLOB';
        sequelizeMock.JSON = 'JSON';
        sequelizeMock.ARRAY = sinon.stub().returns('ARRAY');

        responseMock = {
            toJSON: sinon.stub()
        };
        dataSchemaMock = {
            models: {
                pipeline: {
                    base: joi.object({
                        id: joi.string(),
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
                    indexes: ['str']
                },
                job: {
                    base: joi.object({
                        id: joi.string(),
                        name: joi.string()
                    }),
                    tableName: 'jobs',
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
        mockery.registerMock('sequelize', sequelizeMock);
        mockery.registerMock('screwdriver-data-schema', dataSchemaMock);

        /* eslint-disable global-require */
        Datastore = require('../index');
        /* eslint-enable global-require */
        datastore = new Datastore();
    });

    afterEach(() => {
        mockery.deregisterAll();
        mockery.resetCache();
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
                    type: 'TEXT',
                    primaryKey: true
                },
                name: {
                    type: 'TEXT'
                }
            });
            assert.calledWith(sequelizeClientMock.define, 'pipelines', {
                id: {
                    type: 'TEXT',
                    primaryKey: true
                },
                str: {
                    type: 'TEXT'
                },
                date: {
                    type: 'DATE'
                },
                num: {
                    type: 'DECIMAL'
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

        it('constructs the clients special for postgres', () => {
            sequelizeClientMock.getDialect.returns('postgres');
            datastore = new Datastore({
                dialect: 'postgres'
            });
            assert.calledWith(sequelizeClientMock.define, 'jobs', {
                id: {
                    type: 'TEXT',
                    primaryKey: true
                },
                name: {
                    type: 'TEXT'
                }
            });
            assert.calledWith(sequelizeClientMock.define, 'pipelines', {
                id: {
                    type: 'TEXT',
                    primaryKey: true
                },
                str: {
                    type: 'TEXT'
                },
                date: {
                    type: 'DATE'
                },
                num: {
                    type: 'DECIMAL'
                },
                bool: {
                    type: 'BOOLEAN'
                },
                bin: {
                    type: 'BLOB'
                },
                arr: {
                    type: 'ARRAY'
                },
                obj: {
                    type: 'JSON'
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
                obj: '{"a":"b"}'
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

        it('gets data by id for postgres', () => {
            const testParams = {
                table: 'pipelines',
                params: {
                    id: 'someId'
                }
            };
            const testData = {
                id: 'data',
                key: 'value',
                arr: [1, 2, 3],
                obj: {
                    a: 'b'
                }
            };
            const realData = {
                id: 'data',
                key: 'value',
                arr: [1, 2, 3],
                obj: {
                    a: 'b'
                }
            };

            sequelizeClientMock.getDialect.returns('postgres');
            sequelizeTableMock.findById.resolves(responseMock);
            responseMock.toJSON.returns(testData);
            datastore = new Datastore({
                dialect: 'postgres'
            });

            return datastore.get(testParams).then((data) => {
                assert.deepEqual(data, realData);
                assert.calledWith(sequelizeTableMock.findById, testParams.params.id);
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

            sequelizeTableMock.create.resolves();

            return datastore.save({
                table: 'pipelines',
                params: {
                    id: 'someIdToPutHere',
                    data: {
                        key: 'value',
                        arr: [1, 2, 3],
                        obj: {
                            a: 'b'
                        }
                    }
                }
            }).then((data) => {
                assert.deepEqual(data, expectedResult);
                assert.calledWith(sequelizeTableMock.create, {
                    key: 'value',
                    id: 'someIdToPutHere',
                    arr: '[1,2,3]',
                    obj: '{"a":"b"}'
                });
            });
        });

        it('saves the data for postgres', () => {
            const expectedResult = {
                id: 'someIdToPutHere',
                key: 'value',
                arr: [1, 2, 3],
                obj: {
                    a: 'b'
                }
            };

            sequelizeTableMock.create.resolves();
            sequelizeClientMock.getDialect.returns('postgres');
            datastore = new Datastore({
                dialect: 'postgres'
            });

            return datastore.save({
                table: 'pipelines',
                params: {
                    id: 'someIdToPutHere',
                    data: {
                        key: 'value',
                        arr: [1, 2, 3],
                        obj: {
                            a: 'b'
                        }
                    }
                }
            }).then((data) => {
                assert.deepEqual(data, expectedResult);
                assert.calledWith(sequelizeTableMock.create, {
                    key: 'value',
                    id: 'someIdToPutHere',
                    arr: [1, 2, 3],
                    obj: {
                        a: 'b'
                    }
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
                    data: { targetKey: 'updatedValue' }
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
                            in: [1, 2, 3]
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
                            in: [1, 2, 3]
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
                            in: [1, 2, 3]
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
