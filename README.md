# Datastore Sequelize
[![Version][npm-image]][npm-url] ![Downloads][downloads-image] [![Build Status][status-image]][status-url] [![Open Issues][issues-image]][issues-url] [![Dependency Status][daviddm-image]][daviddm-url] ![License][license-image]

> Datastore implementation for mysql, postgres, sqlite3, and mssql

## Usage

```bash
npm install screwdriver-datastore-sequelize
```

### Initialization

This module takes the same input as the [sequelize class](http://docs.sequelizejs.com/en/latest/api/sequelize/) with the exception of `database`, `username`, and `password`.  Those should be combined into the overall config object.

```js
const Sequelize = require('screwdriver-datastore-sequelize');
const datastore = new Sequelize();
```

#### Define a specific region and credentials to interact with

```js
const Sequelize = require('screwdriver-datastore-sequelize');
const pcGamerDatastore = new Sequelize({
    dialect: 'postgres',
    database: 'banana',
    username: 'coconut',
    password: 'm0nK3Ys&'
});
```

### Methods

See [base class](https://github.com/screwdriver-cd/datastore-base) for more information.

## Testing

```bash
npm test
```

## License

Code licensed under the BSD 3-Clause license. See LICENSE file for terms.

[npm-image]: https://img.shields.io/npm/v/screwdriver-datastore-sequelize.svg
[npm-url]: https://npmjs.org/package/screwdriver-datastore-sequelize
[downloads-image]: https://img.shields.io/npm/dt/screwdriver-datastore-sequelize.svg
[license-image]: https://img.shields.io/npm/l/screwdriver-datastore-sequelize.svg
[issues-image]: https://img.shields.io/github/issues/screwdriver-cd/screwdriver.svg
[issues-url]: https://github.com/screwdriver-cd/screwdriver/issues
[status-image]: https://cd.screwdriver.cd/pipelines/26/badge
[status-url]: https://cd.screwdriver.cd/pipelines/26
[daviddm-image]: https://david-dm.org/screwdriver-cd/datastore-sequelize.svg?theme=shields.io
[daviddm-url]: https://david-dm.org/screwdriver-cd/datastore-sequelize
