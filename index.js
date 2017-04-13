'use strict';

const _ = require('lodash'),
  BbPromise = require('bluebird'),
  dynamodbMigrations = require('dynamodb-migrations'),
  AWS = require('aws-sdk'),
  dynamodbLocal = require('dynamodb-localhost'),
  fs = require('fs'),
  path = require('path');

class ServerlessDynamodbLocal {
  constructor(serverless, options) {
    this.serverless = serverless;
    this.service = serverless.service;
    this.options = options;
    this.provider = 'aws';
    this.commands = {
      dynamodb: {
        commands: {
          create: {
            usage: 'Create a migration template inside the directory given in serverss.yml',
            lifecycleEvents: ['createHandler'],
            options: {
              name: {
                required: true,
                shortcut: 'n',
                usage: 'Name of the template',
              }
            }
          },
          execute: {
            lifecycleEvents: ['executeHandler'],
            usage: 'Execute a particular migration template',
            options: {
              name: {
                required: true,
                shortcut: 'n',
                usage: 'Name of the template'
              },
              force: {
                shortcut: 'f',
                usage: 'Replace existing table'
              },
              region: {
                shortcut: 'r',
                usage: 'not set, uses region defined in severless.yml',
              },
              stage: {
                required: true,
                shortcut: 's',
                usage: 'test | local | development | production'
              }
            }
          },
          executeAll: {
            lifecycleEvents: ['executeAllHandler'],
            options: {
              force: {
                shortcut: 'f',
                usage: 'Replace existing tables'
              },
              region: {
                shortcut: 'r',
                usage: 'if not set, uses region defined in severless.yml',
              },
              stage: {
                shortcut: 's',
                usage: 'test | local | development | production'
              }
            }
          }
        }
      }
    };

    this.hooks = {
      'dynamodb:create:createHandler': this.createHandler.bind(this),
      'dynamodb:execute:executeHandler': this.executeHandler.bind(this),
      'dynamodb:executeAll:executeAllHandler': this.executeAllHandler.bind(this)
    };
  }
  createHandler() {
    let self = this,
      options = this.options;
    return new BbPromise(function(resolve, reject) {
      let dynamodb = self.dynamodbOptions(),
        tableOptions = self.tableOptions();
      dynamodbMigrations.init(dynamodb, tableOptions.path);
      dynamodbMigrations.create(options.name).then(resolve, reject);
    });
  }

  dynamodbOptions(region) {
    let self = this;
    let credentials, config = self.service.custom.dynamodb || {},
      port = config.start && config.start.port || 8000,
      dynamoOptions;
    if(region){
      AWS.config.update({
        region: region
      });
    }else{
      dynamoOptions = {
        endpoint: 'http://localhost:' + port,
        region: 'localhost',
        accessKeyId: 'MOCK_ACCESS_KEY_ID',
        secretAccessKey: 'MOCK_SECRET_ACCESS_KEY'
      };
    }
    return {
      raw: new AWS.DynamoDB(dynamoOptions),
      doc: new AWS.DynamoDB.DocumentClient(dynamoOptions)
    };
  }

  tableOptions(table_prefix, table_suffix) {
    let self = this;
    let config = self.service.custom.dynamodb,
      migration = config && config.migration || {},
      rootPath = self.serverless.config.servicePath,
      path = rootPath + '/' + (migration.dir || 'dynamodb'),
      suffix = table_suffix || migration.table_suffix || '',
      prefix = table_prefix || migration.table_prefix || '';

    return {
      suffix: suffix,
      prefix: prefix,
      path: path
    };
  }

  _createTable(dynamodb, migration) {
    return new BbPromise(function(resolve, reject) {
      dynamodb.raw.createTable(migration.Table, function(err) {
        if (err) {
          if (err.code !== 'ResourceInUseException') {
            console.log('ERROR ' + migration.Table.TableName + ':', 'failed to create table', err.message);
          }
          reject(err);
        } else {
          //console.log("Table creation completed for table: " + migration.Table.TableName);
          resolve(migration);
        }
      });
    });
  }

  deleteTable(dynamodb, TableName) {
    return new BbPromise(function(resolve, reject) {
      dynamodb.raw.deleteTable({ TableName }, function(err) {
        if (err) {
          reject(err);
        } else {
          //console.log('Table deletion completed for table:', TableName);
          resolve();
        }
      });
    });
  }

  runSeeds(dynamodb, migration) {
    if (!migration.Seeds || !migration.Seeds.length) {
      return new BbPromise(function(resolve) {
        resolve(migration);
      });
    } else {
      var params,
        batchSeeds = migration.Seeds.map(function(seed) {
          return {
            PutRequest: {
              Item: seed
            }
          };
        });
      params = {
        RequestItems: {}
      };
      params.RequestItems[migration.Table.TableName] = batchSeeds;
      return new BbPromise(function(resolve, reject) {
        var interval = 0,
          execute = function(interval) {
            setTimeout(function() {
              dynamodb.doc.batchWrite(params, function(err) {
                if (err) {
                  if (err.code === "ResourceNotFoundException" && interval <= 5000) {
                    execute(interval + 1000);
                  } else {
                    reject(err);
                  }
                } else {
                  resolve(migration);
                }
              });
            }, interval);
          };
        execute(interval);
      });
    }
  };

  formatTableName(migration, tableOptions) {
    return tableOptions.prefix + migration.Table.TableName + tableOptions.suffix;
  }

  createTable(dynamodb, tableOptions, options) {
    //console.log('>>>>>>>>>>>>>', JSON.stringify(tableOptions));
    //console.log('=============', JSON.stringify(options));
    var self = this;
    return new BbPromise(function(resolve, reject) {
      var migration = require(tableOptions.path + '/' + options.name + '.js').default();
      migration.Table.TableName = self.formatTableName(migration, tableOptions);
      var create = function(resolve, reject) {
        self._createTable(dynamodb, migration).then(function(executedMigrations) {
          return self.runSeeds(dynamodb, migration);
        }).then(function() {
          console.log(migration.Table.TableName + ' table created and seeded');
          resolve();
        }).catch(function(err) {
          if (err.code === 'ResourceInUseException') {
            console.log('skipped - ' + migration.Table.TableName + ' table already exists (-f to replace)');
            resolve();
          } else {
            reject(err);
          }
        });
      };
      if (options.force) {
        self.deleteTable(dynamodb, migration.Table.TableName).then(function() {
          create(resolve, reject);
        }).catch(function(err) {
          if (err.code === 'ResourceNotFoundException') {
            create(resolve, reject);
          } else {
            reject(err);
          }
        });
      } else {
        create(resolve, reject);
      }
    });
  }

  executeHandler() {
    var self = this;
    var options = this.options;
    options.stage = options.stage || 'local';
    if (!self.isValidStage(options.stage)) {
      console.error('invalid stage');
      return;
    }
    options.region = options.region || this.service.provider.region;
    if (options.stage === 'test' || options.stage === 'local') {
      options.region = null;
    }
    var dynamodb = self.dynamodbOptions(options.region),
      tableOptions = self.tableOptions();
    tableOptions.prefix = options.stage + '_';
    dynamodbMigrations.init(dynamodb, tableOptions.path);
    return self.createTable(dynamodb, tableOptions, options);
  }

  isValidStage(stage) {
    return ['test', 'local', 'development', 'production'].indexOf(stage) !== -1;
  }

  executeAllHandler(isOffline) {
    var sfold = function(l, i, fn, next) {
      function recur(idx, v) {
        if (idx >= l.length) { next(null, v); } else {
          fn(l[idx], v, function(err, r) {
            if (err) { next(err); } else { recur(idx + 1, r); }
          }); } } recur(0, i); };
    var self = this;
    var options = this.options;
    options.stage = options.stage || 'local';
    if (!self.isValidStage(options.stage)) {
      console.error('invalid stage');
      return;
    }
    options.region = options.region || this.service.provider.region;
    if (options.stage === 'test' || options.stage === 'local') {
      options.region = null;
    }
    return new BbPromise(function(resolve, reject) {
      var dynamodb = self.dynamodbOptions(options.region),
        tableOptions = self.tableOptions();
      dynamodbMigrations.init(dynamodb, tableOptions.path);
      tableOptions.prefix = options.stage + '_';
      var migrations = [];
      fs.readdirSync(tableOptions.path).forEach(function(file) {
        if (path.extname(file) === '.js') {
          var migration = require(tableOptions.path + '/' + file).default();
          migrations.push(migration);
        }
      });
      sfold(migrations, null, function(migration, sofar, next) {
        var fullTableName = self.formatTableName(migration, tableOptions);
        options.name = migration.Table.TableName;
        options.n = options.name;
        self.createTable(dynamodb, tableOptions, options)
          .then(function() { next(); })
          .catch(function(err) { next(); });
      }, function(err) {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }
}
module.exports = ServerlessDynamodbLocal;
