/**
 * Mongo API Server for Kunpeng-MongoDB.
 */
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var f = require('util').format;
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');

const admin = process.env.MONGO_ADMIN;
const adminPassword = process.env.MONGO_ADMIN_PASSWORD;
const mongoAddress = process.env.MONGO_DB_ADDRESS;
const mongoPort = process.env.MONGO_DB_PORT;
const replicaSet = process.env.MONGO_DB_REPLICA_SET;
const apiPort = process.env.MONGO_API_SERVER_PORT;
const databaseNumberLimit = 3;

app.use(bodyParser.json());  // for parsing application/json
app.use(bodyParser.urlencoded(
    {extended: true}));  // for parsing application/x-www-form-urlencoded

/**
 * Receives create requests from end users.
 */
app.post('/create', function(req, res) {
  var message = '';
  var dbName = req.body.dbName;
  var user = req.body.user;
  var password = req.body.password;
  if (!user || !dbName) {
    return;
  }
  var url = f(
      'mongodb://%s:%s@%s:%s/?authSource=admin&w=0&readPreference=secondary&replicaSet=%s',
      admin, adminPassword, mongoAddress, mongoPort, replicaSet);
  MongoClient.connect(url, function(err, client) {
    if (err) {
      res.send(err);
      return;
    }
    message += 'Connected successfully to MongoDB.\n';
    checkExist(client, user, password, dbName, message, res);
  });
});

/**
 * Receives delete requests from end users.
 */
app.post('/delete', function(req, res) {
  var message = '';
  var dbName = req.body.dbName;
  var user = req.body.user;
  var password = req.body.password;
  if (!user || !dbName) {
    return;
  }
  var url = f(
      'mongodb://%s:%s@mongo-dev-service.mongo-dev:27017/?authSource=%s&w=0&readPreference=secondary&replicaSet=MainRepSet',
      user, password, dbName);
  MongoClient.connect(url, function(err, client) {
    if (err) {
      res.send(err);
      return;
    }
    message += f('Connected successfully to MongoDB %s.\n', dbName);
    deleteDatabase(client, dbName, user, message, res);
  });
});

/**
 * Creates a new mongo database and an owner user associated with this database.
 * @param {MongoClient} client MongoDB client
 * @param {string} user New database owner user name
 * @param {string} password New database owner user password
 * @param {string} dbName New database name
 * @param {string} message Message that should put in response
 * @param {Response} res Http response
 */
let createDatabase = (client, user, password, dbName, message, res) => {
  const db = client.db(dbName);
  db.addUser(
      user, password, {roles: [{role: 'dbOwner', db: dbName}]},
      function(err, result) {
        if (err) {
          message += 'Error: could not add new user.';
          message += err;
        } else {
          message +=
              f('Successfully created User %s in Database %s.\n', user, dbName);
        }
        res.send(message);
        client.close();
      });
};

/**
 * Checks if this database is already existed.
 * @param {MongoClient} client MongoDB client
 * @param {string} endUser New database owner user name
 * @param {string} password New database owner user password
 * @param {string} newDb New database name
 * @param {string} message Message that should put in response
 * @param {Response} res Http response
 */
let checkExist = (client, endUser, password, newDb, message, res) => {
  var db = client.db('admin');
  db.collection('system.users').find().toArray(function(err, result) {
    if (err) {
      message += err;
      res.send(message);
      return;
    }
    for (var i = 0; i < result.length; i++) {
      if (result[i].db == newDb) {
        message += f('Database %s is already existed.\n', newDb);
        res.send(message);
        return;
      }
    }
    checkNumberOfDatabases(client, endUser, password, newDb, message, res);
  });
};

/**
 * Checks if this user already has too many databases. Right now, each user can
 * create at most 3 databases.
 * @param {MongoClient} client MongoDB client
 * @param {string} endUser New database owner user name
 * @param {string} password New database owner user password
 * @param {string} newDb New database name
 * @param {string} message Message that should put in response
 * @param {Response} res Http response
 */
let checkNumberOfDatabases = (client, endUser, password, newDb, message, res) => {
  var db = client.db('admin');
  var query = {user: endUser};
  db.collection('system.users').find(query).toArray(function(err, result) {
    if (err) {
      message += err;
      res.send(message);
      return;
    }
    if (result.length >= databaseNumberLimit) {
      message +=
          'Sorry, you have already had at least 3 databases. Can not create' +
          'new database.\n';
      res.send(message);
      return;
    } else {
      createDatabase(client, endUser, password, newDb, message, res);
    }
  });
};

/**
 * Removes a new mongo database and all the users in this database.
 * @param {MongoClient} client MongoDB client
 * @param {string} dbName Database to delete
 * @param {string} message Message that should put in response
 * @param {Response} res Http response
 */
let deleteDatabase = (client, dbName, user, message, res) => {
  const db = client.db(dbName);
  db.dropDatabase(function(err, result) {
    if (err) {
      message += err;
      res.end(message);
      return;
    } else {
      db.removeUser(user, null, function(err, result) {
        if (err) {
          message += err;
        } else {
          message += f('Successfully deleted database %s.', dbName);
        }
        res.end(message);
        client.close();
      });
    }
  });
};

// Launch listening server.
app.listen(apiPort, function() {
  console.log('App listening on port ' + apiPort + '!');
});

